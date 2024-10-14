#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "common/async/context_pool.h"

#include <filesystem>
#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "rgw_auth_registry.h"
#include "rgw_aio_throttle.h"
#include "rgw_sal.h"
#include "rgw_sal_store.h"
#include "driver/dbstore/common/dbstore.h"
#include "rgw_sal_d4n.h"

#define dout_subsys ceph_subsys_rgw

const static std::string TEST_DIR = "d4n_filter_tests";

extern "C" {
extern rgw::sal::Driver* newD4NFilter(rgw::sal::Driver* next, boost::asio::io_context& io_context);
}

namespace fs = std::filesystem;
namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

std::string getTestDir() {
  auto test_dir = fs::temp_directory_path() / TEST_DIR;
  return test_dir.string();
}

class Environment* env;

class Environment : public ::testing::Environment {
  public:
    Environment() {}

    virtual ~Environment() {}

    void SetUp() override {
      std::vector<const char*> args;

      cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			CODE_ENVIRONMENT_UTILITY,
			CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

      env->cct.get()->_conf.set_val_or_die("dbstore_db_dir", getTestDir());
      common_init_finish(g_ceph_context);

      dpp = new DoutPrefix(cct->get(), dout_subsys, "D4N Object Directory Test: ");

      redisHost = cct->_conf->rgw_d4n_address; 
    }

    std::string redisHost;
    boost::intrusive_ptr<ceph::common::CephContext> cct;
    DoutPrefixProvider* dpp;
};

class D4NFilterFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      fs::current_path(fs::temp_directory_path());
      fs::create_directory(TEST_DIR);

      conn = new connection{boost::asio::make_strand(io)};
      ASSERT_NE(conn, nullptr);

      /* Run fixture's connection */
      config cfg;
      cfg.addr.host = env->redisHost.substr(0, env->redisHost.find(":"));
      cfg.addr.port = env->redisHost.substr(env->redisHost.find(":") + 1, env->redisHost.length()); 

      conn->async_run(cfg, {}, net::detached);
    } 

    virtual void TearDown() {
      delete conn;
    }

    void create_driver(boost::asio::yield_context yield) {
      const rgw::SiteConfig site_config;
      DriverManager::Config cfg = DriverManager::get_config(true, g_ceph_context);
      cfg.store_name = "dbstore";
      cfg.filter_name = "d4n";
      ceph::async::io_context_pool context_pool(env->cct.get()->_conf->rgw_thread_pool_size);

      auto filterDriver = DriverManager::get_raw_storage(env->dpp, g_ceph_context,
						          cfg, io, site_config);

      rgw::sal::Driver* next = filterDriver;
      driver = newD4NFilter(next, io);
      auto d4nFilter = dynamic_cast<rgw::sal::D4NFilterDriver*>(driver);
      dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});
      driver->initialize(env->cct.get(), env->dpp);

      ASSERT_NE(driver, nullptr);
    }

    int create_user(boost::asio::yield_context yield) {
      rgw_user u("test_tenant", "test_user", "ns");

      testUser = driver->get_user(u);
      testUser->get_info().user_id = u;

      return testUser->store_user(env->dpp, optional_yield{yield}, false);
    }

    int create_bucket(boost::asio::yield_context yield) {
      rgw_bucket b;
      init_bucket(&b, "test_tenant", "test_name", "test_data_pool", "test_index_pool", "test_marker", "test_id");

      return driver->load_bucket(env->dpp, b, &testBucket, optional_yield{yield});
    }

    void put_object(std::string name, boost::asio::yield_context yield) {
      std::string object_name = "test_object_" + name;
      std::unique_ptr<rgw::sal::Object> obj = testBucket->get_object(rgw_obj_key(object_name));
      ASSERT_NE(obj.get(), nullptr);

      rgw_owner owner;
      ACLOwner acl_owner;
      rgw_placement_rule ptail_placement_rule;
      uint64_t olh_epoch = 123;
      std::string unique_tag;

      rgw_user uid{"test_tenant", "test_filter"};
      owner = uid;
      acl_owner.id = owner; 
      testWriter = driver->get_atomic_writer(env->dpp, 
					      optional_yield{yield},
					      obj.get(),
					      acl_owner,
					      &ptail_placement_rule,
					      olh_epoch,
					      "test_filter");
  
      size_t accounted_size = 4;
      std::string etag("test_etag");
      ceph::real_time mtime; 
      ceph::real_time set_mtime;

      buffer::list bl;
      std::string tmp = "test_attrs_value_" + name;
      bl.append("test_attrs_value_" + name);
      std::map<std::string, bufferlist> attrs{{"test_attrs_key_" + name, bl}};

      rgw::cksum::Cksum cksum;
      ceph::real_time delete_at;
      std::string user_data;
      rgw_zone_set zones_trace;
      const req_context rctx{env->dpp, optional_yield{yield}, nullptr};
      
      ASSERT_EQ(testWriter->prepare(optional_yield{yield}), 0);
      ASSERT_EQ(testWriter->process({}, 0), 0);
      ASSERT_EQ(testWriter->complete(accounted_size, etag,
				       &mtime, set_mtime,
				       attrs, cksum,
				       delete_at,
				       nullptr, nullptr, nullptr,
				       nullptr, nullptr, rctx, 0), 0);
    }

    net::io_context io;
    connection* conn; 

    CephContext* cct;
    rgw::sal::Driver* driver;
    std::unique_ptr<rgw::sal::User> testUser = nullptr;
    std::unique_ptr<rgw::sal::Bucket> testBucket = nullptr;
    std::unique_ptr<rgw::sal::Writer> testWriter = nullptr;
};

void rethrow(std::exception_ptr eptr) {
  if (eptr) std::rethrow_exception(eptr);
}

class DriverDestructor {
  rgw::sal::Driver* driver;

public:
  explicit DriverDestructor(rgw::sal::D4NFilterDriver* _s) : driver(_s) {}
  ~DriverDestructor() {
    DriverManager::close_storage(driver);
  }
};

TEST_F(D4NFilterFixture, PutYield)
{

  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    std::vector<std::string> fields;
    fields.push_back("test_attrs_key_PutObject");
    create_driver(yield);

    ASSERT_EQ(create_user(yield), 0);
    ASSERT_EQ(create_bucket(yield), -2); // -ENOENT since bucket doesn't exist, which is expected
    
    put_object("PutObject", yield);

    boost::system::error_code ec;
    request req;
    req.push("PING", "hello");
    response<std::string> resp;
    conn->async_exec(req, resp, yield[ec]);
/*
    req.push("HGETALL", location + "test_object_PutObject");
    req.push_range("HMGET", location + "test_object_PutObject", fields);
    req.push("FLUSHALL");

    response< std::map<std::string, std::string>, 
	      std::map<std::string, std::string>,
	      boost::redis::ignore_t > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value().size(), 24);
    EXPECT_EQ(std::get<1>(resp).value().begin()->second, "test_attrs_value_PutObject");
*/
    conn->cancel();

    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
