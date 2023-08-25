#pragma once

#include "rgw_common.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>

#include <boost/lexical_cast.hpp>
#include <boost/redis/connection.hpp>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace d4n {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

struct Address {
  std::string host;
  int port;
};

struct CacheObj {
  std::string objName; /* S3 object name */
  std::string bucketName; /* S3 bucket name */
  time_t creationTime; /* Creation time of the S3 Object */
  bool dirty;
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of object locations for multiple backends */
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t version; /* RADOS object block ID */
  uint64_t size; /* Block size in bytes */
  int globalWeight = 0; /* LFUDA policy variable */
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of block locations */
};

class Directory {
  public:
    Directory() {}
    CephContext* cct;
};

class ObjectDirectory: public Directory { // weave into write workflow -Sam
  public:
    ObjectDirectory(net::io_context& io_context) {
      conn = new connection{boost::asio::make_strand(io_context)};
    }
    ObjectDirectory(net::io_context& io_context, std::string host, int port) {
      conn = new connection{boost::asio::make_strand(io_context)};
    }

    int init(CephContext* cct, const DoutPrefixProvider* dpp) {
      this->cct = cct;

      config cfg;
      cfg.addr.host = cct->_conf->rgw_d4n_host; // same or different address from block directory? -Sam
      cfg.addr.port = std::to_string(cct->_conf->rgw_d4n_port);

      if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
	ldpp_dout(dpp, 10) << "D4N Directory " << __func__ << ": Object directory endpoint was not configured correctly" << dendl;
	return -EDESTADDRREQ;
      }

      conn->async_run(cfg, {}, net::detached);
      return 0;
    }

    int find_client(cpp_redis::client* client);
    int exist_key(std::string key);
    Address get_addr() { return addr; }

    int set(CacheObj* object);
    int get(CacheObj* object);
    int copy(CacheObj* object, CacheObj* copyObject);
    int del(CacheObj* object);

  private:
    connection* conn;
    cpp_redis::client client;
    Address addr;
    std::string build_index(CacheObj* object);
};

class BlockDirectory: public Directory {
  public:
    BlockDirectory(net::io_context& io_context) {
      conn = std::make_shared<connection>(boost::asio::make_strand(io_context));
    }
    BlockDirectory(net::io_context& io_context, std::string host, int port) {
      conn = std::make_shared<connection>(boost::asio::make_strand(io_context));
    }
    
    int init(CephContext* cct, const DoutPrefixProvider* dpp) {
      this->cct = cct;

      config cfg;
      cfg.addr.host = cct->_conf->rgw_d4n_host;
      cfg.addr.port = std::to_string(cct->_conf->rgw_d4n_port);

      if (!cfg.addr.host.length() || !cfg.addr.port.length()) { // add logs to other methods -Sam
	ldpp_dout(dpp, 10) << "D4N Directory " << __func__ << ": Block directory endpoint was not configured correctly" << dendl;
	return -EDESTADDRREQ;
      }

      conn->async_run(cfg, {}, net::detached); 
      return 0;
    }
	
    int exist_key(std::string key, optional_yield y);
    void shutdown();

    int set(CacheBlock* block, optional_yield y);
    int get(CacheBlock* block, optional_yield y);
    int copy(CacheBlock* block, std::string copyName, std::string copyBucketName, optional_yield y);
    int del(CacheBlock* block, optional_yield y);

    int update_field(CacheBlock* block, std::string field, std::string value, optional_yield y);

  private:
    std::shared_ptr<connection> conn;

    std::string build_index(CacheBlock* block);
};

} } // namespace rgw::d4n
