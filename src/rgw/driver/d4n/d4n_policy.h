#pragma once

#include <string>
#include <iostream>
#include <cpp_redis/cpp_redis>
#include <boost/redis/connection.hpp>

#include "rgw_common.h"
#include "d4n_directory.h"
#include "../../rgw_redis_driver.h"

namespace rgw { namespace d4n {

class CachePolicy {
  private:
    cpp_redis::client client;
    Address addr;

  public:
    CephContext* cct;

    CachePolicy() : addr() {}
    virtual ~CachePolicy() = default;

    virtual void init(CephContext *_cct) {
      cct = _cct;
      addr.host = cct->_conf->rgw_d4n_host;
      addr.port = cct->_conf->rgw_d4n_port;
    }
    virtual int find_client(const DoutPrefixProvider* dpp, cpp_redis::client* client) = 0;
    virtual int exist_key(std::string key) = 0;
    virtual Address get_addr() { return addr; }
    virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y) = 0;
    virtual uint64_t eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y) = 0;
};

class LFUDAPolicy : public CachePolicy {
  private:
    cpp_redis::client client;

  public:
    LFUDAPolicy() : CachePolicy() {}

    int set_age(int age);
    int get_age();
    int set_global_weight(std::string key, int weight);
    int get_global_weight(std::string key);
    int set_min_avg_weight(size_t weight, std::string cacheLocation);
    int get_min_avg_weight();
    CacheBlock find_victim(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y);

    virtual int find_client(const DoutPrefixProvider* dpp, cpp_redis::client* client) override { return CachePolicy::find_client(dpp, client); }
    virtual int exist_key(std::string key) override { return CachePolicy::exist_key(key); }
    virtual int get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
    virtual uint64_t eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y) override;
};

class PolicyDriver {
  private:
    std::string policyName;

  public:
    CachePolicy* cachePolicy;

    PolicyDriver(std::string _policyName) : policyName(_policyName) {}
    ~PolicyDriver() {
      delete cachePolicy;
    }

    int init();
};

} } // namespace rgw::d4n