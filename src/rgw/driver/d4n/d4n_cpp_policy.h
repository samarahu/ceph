#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/system/detail/errc.hpp>

#include "rgw_sal_d4n.h"
#include "driver/d4n/d4n_hashing.h"
#include "rgw_cache_driver.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {
  class D4NFilterObject;
}

namespace rgw { namespace d4n {

namespace asio = boost::asio;
namespace sys = boost::system;


class RGWCachePolicy {
  friend uint16_t crc16(const char *buf, int len);
  friend unsigned int hash_slot(const char *key, int keylen);
  protected:
    struct Entry : public boost::intrusive::list_base_hook<> {
      std::string key;
      uint64_t offset;
      uint64_t len;
      std::string version;
      bool dirty;
      time_t creationTime;
      rgw_user user;
      Entry(std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, rgw_user user) : key(key), offset(offset), 
                                                                                     len(len), version(version), dirty(dirty), creationTime(creationTime), user(user) {}
    };

   
    //The disposer object function
    struct Entry_delete_disposer {
      void operator()(Entry *e) {
        delete e;
      }
    };

    struct ObjEntry : public boost::intrusive::list_base_hook<> {
      std::string key;
      std::string version;
      bool dirty;
      uint64_t size;
      time_t creationTime;
      rgw_user user;
      std::string etag;
      ObjEntry(std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, rgw_user user, std::string& etag) : key(key), version(version), dirty(dirty), size(size), creationTime(creationTime), user(user), etag(etag) {}
    };

    struct ObjEntry_delete_disposer {
      void operator()(ObjEntry *e) {
        delete e;
      }
    };

  public:
    RGWCachePolicy() {}
    virtual ~RGWCachePolicy() = default; 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver) = 0;
    //virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, rgw::sal::Driver *_driver) = 0;
    virtual int exist_key(std::string key) = 0;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) = 0;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, const rgw_user user, optional_yield y) = 0;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, optional_yield y) = 0;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual void cleaning(const DoutPrefixProvider* dpp) = 0;
};

class RGWLFUDAPolicy : public RGWCachePolicy {
  private:
    template<typename T>
    struct EntryComparator {
      bool operator()(T* const e1, T* const e2) const {
	return e1->localWeight > e2->localWeight;
      }
    }; 

    struct LFUDAEntry : public Entry {
      int localWeight;
      using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
      handle_type handle;

      LFUDAEntry(std::string& key, uint64_t offset, uint64_t len, std::string& version, bool dirty, time_t creationTime, rgw_user user, int localWeight) : Entry(key, offset, len, version, dirty, creationTime, user), localWeight(localWeight) {}
      
      void set_handle(handle_type handle_) { handle = handle_; } 
    };

    struct LFUDAObjEntry : public ObjEntry {
      LFUDAObjEntry(std::string& key, std::string& version, bool dirty, uint64_t size, time_t creationTime, rgw_user user, std::string& etag) : ObjEntry(key, version, dirty, size, creationTime, user, etag) {}
    };

    using Heap = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>;
    Heap entries_heap;
    std::unordered_map<std::string, LFUDAEntry*> entries_map;
    std::unordered_map<std::string, LFUDAObjEntry*> o_entries_map;
    std::mutex lfuda_lock;

    int age = 1, weightSum = 0, postedSum = 0;
    optional_yield y = null_yield;

    //net::io_context& io;
    //std::shared_ptr<connection> conn;
    std::shared_ptr<cpp_redis::client[]> client_conn;
    RGWBlockDirectory* dir;
    rgw::cache::CacheDriver* cacheDriver;
    std::optional<asio::steady_timer> rthread_timer;
    rgw::sal::Driver *driver;
    std::thread tc;
    std::thread lfuda_t;
    CephContext *cct;

    void connectClient();
    int findClient(std::string key);
    int exist_field(std::string key, std::string field);
    int set(std::string key, std::string field, std::string value);
    std::string get(std::string key, std::string field);
    
    int sendRemote(const DoutPrefixProvider* dpp, CacheBlockCpp *victim, std::string remoteCacheAddress, std::string key, bufferlist* out_bl, optional_yield y);
    int getMinAvgWeight(const DoutPrefixProvider* dpp, int* minAvgWeight, std::string* cache_address, optional_yield y);
    CacheBlockCpp* get_victim_block(const DoutPrefixProvider* dpp, optional_yield y);
    int age_sync(const DoutPrefixProvider* dpp, optional_yield y); 
    int local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y); 
    void redis_sync(const DoutPrefixProvider* dpp, optional_yield y);

    /*
    void rthread_stop() {
      std::lock_guard l{lfuda_lock};

      if (rthread_timer) {
	rthread_timer->cancel();
      }
    }
    */

    LFUDAEntry* find_entry(std::string key) { 
      auto it = entries_map.find(key); 
      if (it == entries_map.end())
        return nullptr;
      return it->second;
    }

  public:
    RGWLFUDAPolicy(std::shared_ptr<cpp_redis::client[]>& conn, rgw::cache::CacheDriver* cacheDriver) : RGWCachePolicy(), 
    //LFUDAPolicy(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver) : CachePolicy(), 
											   //io(io_context),
											   client_conn(conn),
											   cacheDriver(cacheDriver)
    {
      //dir = new BlockDirectory{conn};
      dir = new RGWBlockDirectory{conn};
    }
    ~RGWLFUDAPolicy() {
      //rthread_stop();
      //shutdown();
      delete dir;
    } 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver) override; 

    virtual int exist_key(std::string key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, const rgw_user user, optional_yield y) override;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override;
    void save_y(optional_yield y) { this->y = y; }
    //void shutdown();
};

class RGWLRUPolicy : public RGWCachePolicy {
  private:
    typedef boost::intrusive::list<Entry> List;

    std::unordered_map<std::string, Entry*> entries_map;
    std::unordered_map<std::string, ObjEntry*> o_entries_map;
    std::mutex lru_lock;
    List entries_lru_list;
    rgw::cache::CacheDriver* cacheDriver;

    bool _erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y);

  public:
    RGWLRUPolicy(rgw::cache::CacheDriver* cacheDriver) : cacheDriver{cacheDriver} {}

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver* _driver) override { return 0; } 
    virtual int exist_key(std::string key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, const rgw_user user, optional_yield y) override;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override {}
};

class RGWPolicyDriver {
  private:
    std::string policyName;
    RGWCachePolicy* cachePolicy;

  public:
    RGWPolicyDriver(std::shared_ptr<cpp_redis::client[]>& conn, rgw::cache::CacheDriver* cacheDriver, std::string _policyName) : policyName(_policyName) 
    {
      if (policyName == "lfuda") {
	cachePolicy = new RGWLFUDAPolicy(conn, cacheDriver);
	//cachePolicy = new LFUDAPolicy(io_context, cacheDriver);
      } else if (policyName == "lru") {
	cachePolicy = new RGWLRUPolicy(cacheDriver);
      }
    }
    ~RGWPolicyDriver() {
      delete cachePolicy;
    }

    RGWCachePolicy* get_cache_policy() { return cachePolicy; }
    std::string get_policy_name() { return policyName; }
};

} } // namespace rgw::d4n
