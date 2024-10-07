#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "rgw_auth_registry.h"
#include "rgw_aio_throttle.h"
#include "rgw_sal.h"
#include "rgw_sal_store.h"
#include "driver/dbstore/common/dbstore.h"
#include "rgw_sal_d4n.h"

#define dout_subsys ceph_subsys_rgw

extern "C" {
extern rgw::sal::Driver* newD4NFilter(rgw::sal::Driver* next, boost::asio::io_context& io_context);
}

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

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
      cfg.store_name = "dbstore";
      cfg.filter_name = "d4n";

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

    int put_object(std::string name, boost::asio::yield_context yield) {
      std::string object_name = "test_object_" + name;
      std::unique_ptr<rgw::sal::Object> obj = testBucket->get_object(rgw_obj_key(object_name));
      const ACLOwner owner;
      rgw_placement_rule ptail_placement_rule;
      uint64_t olh_epoch = 123;
      std::string unique_tag;

      obj->get_obj_attrs(optional_yield{yield}, env->dpp);

      testWriter = driver->get_atomic_writer(env->dpp, 
					      optional_yield{yield},
					      obj.get(),
					      owner,
					      &ptail_placement_rule,
					      olh_epoch,
					      unique_tag);
  
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
      char if_match;
      char if_nomatch;
      std::string user_data;
      rgw_zone_set zones_trace;
      bool canceled;
      const req_context rctx{env->dpp, optional_yield{yield}, nullptr};
      
      if (testWriter->prepare(optional_yield{yield}) < 0) {
        std::cout << "Sam 1" << std::endl;
	if (testWriter->process({}, 0) < 0) {
	  std::cout << "Sam 2" << std::endl;
	  return testWriter->complete(accounted_size, etag,
				       &mtime, set_mtime,
				       attrs, cksum,
				       delete_at,
				       &if_match, &if_nomatch,
				       &user_data,
				       &zones_trace, &canceled,
				       rctx, rgw::sal::FLAG_LOG_OP);
	} else {
	  return -1;
	}
      } else {
        return -1;
      }
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
    
    EXPECT_EQ(put_object("PutObject", yield), 0);
    //EXPECT_NE(testWriter, nullptr);

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

#if 0
TEST_F(D4NFilterFixture, GetYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "RedisCache/testName", "data", "new data", "attr", "newVal");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    bufferlist ret;
    rgw::sal::Attrs retAttrs;

    ASSERT_EQ(0, cacheDriver->get(env->dpp, "testName", 0, bl.length(), ret, retAttrs, yield));
    EXPECT_EQ(ret.to_str(), "new data");
    EXPECT_EQ(retAttrs.begin()->second.to_str(), "newVal");
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, PutAsyncYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    std::unique_ptr<rgw::Aio> aio = rgw::make_throttle(cct->_conf->rgw_get_obj_window_size, yield);
    auto completed = cacheDriver->put_async(env->dpp, yield, aio.get(), "testName", bl, bl.length(), attrs, 0, 0);
    drain(env->dpp, aio.get());

    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HMGET", "RedisCache/testName", "attr", "data");
    req.push("FLUSHALL");
    response<std::vector<std::string>, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], "attrVal");
    EXPECT_EQ(std::get<0>(resp).value()[1], "test data");
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, GetAsyncYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    std::unique_ptr<rgw::Aio> aio = rgw::make_throttle(cct->_conf->rgw_get_obj_window_size, yield);
    auto completed = cacheDriver->get_async(env->dpp, yield, aio.get(), "testName", 0, bl.length(), 0, 0);
    drain(env->dpp, aio.get());

    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HMGET", "RedisCache/testName", "attr", "data");
    req.push("FLUSHALL");
    response<std::vector<std::string>, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], "attrVal");
    EXPECT_EQ(std::get<0>(resp).value()[1], "test data");
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, DeleteDataYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "RedisCache/testName");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, cacheDriver->delete_data(env->dpp, "testName", yield));
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "RedisCache/testName");
      req.push("FLUSHALL");
      response<int, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, AppendDataYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HGET", "RedisCache/testName", "data");
      response<std::string> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), "test data");
    }

    bufferlist val;
    val.append(" has been written");

    ASSERT_EQ(0, cacheDriver->append_data(env->dpp, "testName", val, yield));
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("HGET", "RedisCache/testName", "data");
      req.push("FLUSHALL");
      response<std::string, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), "test data has been written");
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, RenameYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{yield}));
    ASSERT_EQ(0, cacheDriver->rename(env->dpp, "testName", "newTestName", optional_yield{yield}));
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "RedisCache/testName");
      req.push("EXISTS", "RedisCache/newTestName");
      req.push("FLUSHALL");
      response<int, int, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
      EXPECT_EQ(std::get<1>(resp).value(), 1);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, SetAttrsYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    rgw::sal::Attrs newAttrs;
    bufferlist newVal;
    newVal.append("newVal");
    newAttrs.insert({"newAttr", newVal});
     
    newVal.clear();
    newVal.append("nextVal");
    newAttrs.insert({"nextAttr", newVal});

    ASSERT_EQ(0, cacheDriver->set_attrs(env->dpp, "testName", newAttrs, yield));
    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HMGET", "RedisCache/testName", "newAttr", "nextAttr");
    req.push("FLUSHALL");

    response< std::vector<std::string>,
              boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], "newVal");
    EXPECT_EQ(std::get<0>(resp).value()[1], "nextVal");
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, GetAttrsYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    rgw::sal::Attrs nextAttrs = attrs;
    bufferlist nextVal;
    nextVal.append("nextVal");
    nextAttrs.insert({"nextAttr", nextVal});

    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), nextAttrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "RedisCache/testName", "attr", "newVal1", "nextAttr", "newVal2");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    rgw::sal::Attrs retAttrs;

    ASSERT_EQ(0, cacheDriver->get_attrs(env->dpp, "testName", retAttrs, yield));
   
    auto it = retAttrs.begin();
    EXPECT_EQ(it->second.to_str(), "newVal1");
    ++it;
    EXPECT_EQ(it->second.to_str(), "newVal2");
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, UpdateAttrsYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    rgw::sal::Attrs newAttrs;
    bufferlist newVal;
    newVal.append("newVal");
    newAttrs.insert({"attr", newVal});

    ASSERT_EQ(0, cacheDriver->update_attrs(env->dpp, "testName", newAttrs, yield));
    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HGET", "RedisCache/testName", "attr");
    req.push("FLUSHALL");
    response<std::string, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), "newVal");

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, DeleteAttrsYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", "RedisCache/testName", "attr");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    rgw::sal::Attrs delAttrs;
    bufferlist delVal;
    delAttrs.insert({"attr", delVal});

    ASSERT_GE(cacheDriver->delete_attrs(env->dpp, "testName", delAttrs, yield), 0);
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", "RedisCache/testName", "attr");
      req.push("FLUSHALL");
      response<int, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, SetAttrYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));
    ASSERT_GE(cacheDriver->set_attr(env->dpp, "testName", "newAttr", "newVal", yield), 0);
    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HGET", "RedisCache/testName", "newAttr");
    req.push("FLUSHALL");

    response<std::string, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), "newVal");
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, GetAttrYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "RedisCache/testName", "attr", "newVal");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }
    std::string attr_val;
    ASSERT_EQ(0, cacheDriver->get_attr(env->dpp, "testName", "attr", attr_val, yield));
    ASSERT_EQ("newVal", attr_val);
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}
#endif

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
