#include <boost/algorithm/string.hpp>
#include "rgw_redis_driver.h"
//#include "rgw_ssd_driver.h" // fix -Sam
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/redis/src.hpp>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace cache { 

std::unordered_map<std::string, Partition> RedisDriver::partitions;

std::vector< std::pair<std::string, std::string> > build_attrs(rgw::sal::Attrs* binary) 
{
  std::vector< std::pair<std::string, std::string> > values;
  rgw::sal::Attrs::iterator attrs;

  /* Convert to vector */
  if (binary != NULL) {
    for (attrs = binary->begin(); attrs != binary->end(); ++attrs) {
      values.push_back(std::make_pair(attrs->first, attrs->second.to_str()));
    }
  }

  return values;
}

int RedisDriver::find_client(const DoutPrefixProvider* dpp) 
{
  if (client.is_connected())
    return 0;

  if (addr.host == "" || addr.port == 0) { 
    ldpp_dout(dpp, 10) << "RGW Redis Cache: Redis cache endpoint was not configured correctly" << dendl;
    return -EDESTADDRREQ;
  }

  client.connect(addr.host, addr.port, nullptr);

  if (!client.is_connected())
    return -ECONNREFUSED;

  return 0;
}

int RedisDriver::insert_entry(const DoutPrefixProvider* dpp, std::string key, off_t offset, uint64_t len) 
{
  auto ret = entries.emplace(key, Entry(key, offset, len));
  return ret.second;
}

std::optional<Entry> RedisDriver::get_entry(const DoutPrefixProvider* dpp, std::string key) 
{
  auto iter = entries.find(key);

  if (iter != entries.end()) {
    return iter->second;
  }

  return std::nullopt;
}

int RedisDriver::remove_entry(const DoutPrefixProvider* dpp, std::string key) 
{
  return entries.erase(key);
}

int RedisDriver::add_partition_info(Partition& info)
{
  std::string key = info.name + info.type;
  auto ret = partitions.emplace(key, info);

  return ret.second;
}

int RedisDriver::remove_partition_info(Partition& info)
{
  std::string key = info.name + info.type;
  return partitions.erase(key);
}

auto RedisDriver::redis_exec(boost::system::error_code ec, boost::redis::request req, boost::redis::response<std::string>& resp, optional_yield y) {
  assert(y); 
  auto yield = y.get_yield_context();

  return conn.async_exec(req, resp, yield[ec]);
}

bool RedisDriver::key_exists(const DoutPrefixProvider* dpp, const std::string& key) 
{
  int result;
  std::string entry = partition_info.location + key;
  std::vector<std::string> keys;
  keys.push_back(entry);

  if (!client.is_connected()) 
    find_client(dpp);

  try {
    client.exists(keys, [&result](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        result = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {}

  return result;
}

std::vector<Entry> RedisDriver::list_entries(const DoutPrefixProvider* dpp) 
{
  std::vector<Entry> result;

  for (auto it = entries.begin(); it != entries.end(); ++it) 
    result.push_back(it->second);

  return result;
}

size_t RedisDriver::get_num_entries(const DoutPrefixProvider* dpp) 
{
  return entries.size();
}

/*
uint64_t RedisDriver::get_free_space(const DoutPrefixProvider* dpp) 
{
  int result = -1;

  if (!client.is_connected()) 
    find_client(dpp);

  try {
    client.info([&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        int usedMem = -1;
	int maxMem = -1;

        std::istringstream iss(reply.as_string());
	std::string line;    
        while (std::getline(iss, line)) {
	  size_t pos = line.find_first_of(":");
	  if (pos != std::string::npos) {
	    if (line.substr(0, pos) == "used_memory") {
	      usedMem = std::stoi(line.substr(pos + 1, line.length() - pos - 2));
	    } else if (line.substr(0, line.find_first_of(":")) == "maxmemory") {
	      maxMem = std::stoi(line.substr(pos + 1, line.length() - pos - 2));
	    } 
	  }
        }

	if (usedMem > -1 && maxMem > -1)
	  result = maxMem - usedMem;
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return result;
}
*/

std::optional<Partition> RedisDriver::get_partition_info(const DoutPrefixProvider* dpp, const std::string& name, const std::string& type)
{
  std::string key = name + type;

  auto iter = partitions.find(key);
  if (iter != partitions.end())
    return iter->second;

  return std::nullopt;
}

std::vector<Partition> RedisDriver::list_partitions(const DoutPrefixProvider* dpp)
{
  std::vector<Partition> partitions_v;

  for (auto& it : partitions)
    partitions_v.emplace_back(it.second);

  return partitions_v;
}

/* Currently an attribute but will also be part of the Entry metadata once consistency is guaranteed -Sam
int RedisDriver::update_local_weight(const DoutPrefixProvider* dpp, std::string key, int localWeight) 
{
  auto iter = entries.find(key);

  if (iter != entries.end()) {
    iter->second.localWeight = localWeight;
    return 0;
  }

  return -1;
}
*/

int RedisDriver::initialize(CephContext* cct, const DoutPrefixProvider* dpp) 
{
  namespace net = boost::asio;
  using boost::redis::config;

  this->cct = cct;

  if (partition_info.location.back() != '/') {
    partition_info.location += "/";
  }

  // remove
  addr.host = cct->_conf->rgw_d4n_host; // change later -Sam
  addr.port = cct->_conf->rgw_d4n_port;

  if (addr.host == "" || addr.port == 0) {
    ldpp_dout(dpp, 10) << "RGW Redis Cache: Redis cache endpoint was not configured correctly" << dendl;
    return -EDESTADDRREQ;
  }

  config cfg;
  cfg.addr.host = addr.host;
  cfg.addr.port = std::to_string(addr.port);

  conn.async_run(cfg, {}, net::detached);

/*  client.connect("127.0.0.1", 6379, nullptr);

  if (!client.is_connected()) {
    ldpp_dout(dpp, 10) << "RGW Redis Cache: Could not connect to redis cache endpoint." << dendl;
    return ECONNREFUSED;
  }*/

  return 0;
}

int RedisDriver::put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  /* Every set will be treated as new */ // or maybe, if key exists, simply return? -Sam
  try {
    std::string result; 
    auto redisAttrs = build_attrs(&attrs);
    redisAttrs.push_back({"data", bl.to_str()});

    client.hmset(entry, redisAttrs, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
	result = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    if (result != "OK") {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return insert_entry(dpp, key, 0, len); // why is offset necessarily 0? -Sam
}

int RedisDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  
  if (!client.is_connected()) 
    find_client(dpp);
    
  if (key_exists(dpp, key)) {
    /* Retrieve existing values from cache */
    try {
      client.hgetall(entry, [&bl, &attrs](cpp_redis::reply &reply) {
	if (reply.is_array()) {
	  auto arr = reply.as_array();
    
	  if (!arr[0].is_null()) {
    	    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      if (arr[i].as_string() == "data") {
                bl.append(arr[i + 1].as_string());
	      } else {
	        buffer::list bl_value;
		bl_value.append(arr[i + 1].as_string());
                attrs.insert({arr[i].as_string(), bl_value});
		bl_value.clear();
	      }
            }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object was not retrievable." << dendl;
    return -2;
  }

  return 0;
}

int RedisDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data, optional_yield y) 
{
  namespace net = boost::asio;
  using boost::redis::request;
  using boost::redis::response;

  std::string value;
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  if (key_exists(dpp, key)) {
    try {
      client.hget(entry, "data", [&value](cpp_redis::reply &reply) {
        if (!reply.is_null()) {
          value = reply.as_string();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }
  }

  try { // do we want key check here? -Sam
    std::string location = partition_info.location + key;
    std::string newVal = value + bl_data.to_str();

    request req;
    response<std::string> resp;

    req.push("HMSET", location, "data", newVal);

    conn.async_exec(req, resp, [&](auto ec, auto) {
       if (!ec)
	  dout(0) << "Sam: " << std::get<0>(resp).value() << dendl;

       conn.cancel();
    });

    /* Append to existing value or set as new value */
   /* std::string newVal = value + bl_data.to_str();
    std::vector< std::pair<std::string, std::string> > field;
    field.push_back({"data", newVal});
    std::string result;

    client.hmset(entry, field, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    if (result != "OK") {
      return -1;
    }*/
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int RedisDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  if (key_exists(dpp, key)) {
    int exists = -2;

    try {
      client.hexists(entry, "data", [&exists](cpp_redis::reply &reply) {
	if (!reply.is_null()) {
	  exists = reply.as_integer();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }

    if (exists) {
      try {
	int result;
	std::vector<std::string> deleteField;
	deleteField.push_back("data");

	client.hdel(entry, deleteField, [&result](cpp_redis::reply &reply) {
	  if (reply.is_integer()) {
	    result = reply.as_integer(); 
	  }
	});

	client.sync_commit(std::chrono::milliseconds(1000));

	if (!result) {
	  return -1;
	} else {
	  return remove_entry(dpp, key);
	}
      } catch(std::exception &e) {
	return -1;
      }
    } else {
      return 0; /* No delete was necessary */
    }
  } else {
    return 0; /* No delete was necessary */
  }
}

int RedisDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  if (key_exists(dpp, key)) {
    try {
      client.hgetall(entry, [&attrs](cpp_redis::reply &reply) {
	if (reply.is_array()) { 
	  auto arr = reply.as_array();
    
	  if (!arr[0].is_null()) {
    	    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      if (arr[i].as_string() != "data") {
	        buffer::list bl_value;
		bl_value.append(arr[i + 1].as_string());
                attrs.insert({arr[i].as_string(), bl_value});
		bl_value.clear();
	      }
            }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object was not retrievable." << dendl;
    return -2;
  }

  return 0;
}

int RedisDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) 
{
  if (attrs.empty())
    return -1;
      
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  if (key_exists(dpp, key)) {
    /* Every attr set will be treated as new */
    try {
      std::string result;
      auto redisAttrs = build_attrs(&attrs);
	
      client.hmset(entry, redisAttrs, [&result](cpp_redis::reply &reply) {
	if (!reply.is_null()) {
	  result = reply.as_string();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (result != "OK") {
	return -1;
      }
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object was not retrievable." << dendl;
    return -2;
  }

  return 0;
}

int RedisDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  if (key_exists(dpp, key)) {
    try {
      std::string result;
      auto redisAttrs = build_attrs(&attrs);

      client.hmset(entry, redisAttrs, [&result](cpp_redis::reply &reply) {
        if (!reply.is_null()) {
          result = reply.as_string();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (result != "OK") {
        return -1;
      }
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object was not retrievable." << dendl;
    return -2;
  }

  return 0;
}

int RedisDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  if (key_exists(dpp, key)) {
    std::vector<std::string> getFields;

    try {
      client.hgetall(entry, [&getFields](cpp_redis::reply &reply) {
	if (reply.is_array()) {
	  auto arr = reply.as_array();
    
	  if (!arr[0].is_null()) {
	    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      getFields.push_back(arr[i].as_string());
	    }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }

    auto redisAttrs = build_attrs(&del_attrs);
    std::vector<std::string> redisFields;

    std::transform(begin(redisAttrs), end(redisAttrs), std::back_inserter(redisFields),
      [](auto const& pair) { return pair.first; });

    /* Only delete attributes that have been stored */
    for (const auto& it : redisFields) {
      if (std::find(getFields.begin(), getFields.end(), it) == getFields.end()) {
        redisFields.erase(std::find(redisFields.begin(), redisFields.end(), it));
      }
    }

    try {
      int result = 0;

      client.hdel(entry, redisFields, [&result](cpp_redis::reply &reply) {
        if (reply.is_integer()) {
          result = reply.as_integer();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      return result - 1;
    } catch(std::exception &e) {
      return -1;
    }
  }

  ldpp_dout(dpp, 20) << "RGW Redis Cache: Object is not in cache." << dendl;
  return -2;
}

std::string RedisDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  std::string attrValue;

  if (!client.is_connected()) 
    find_client(dpp);

  if (key_exists(dpp, key)) {
    int exists = -2;
    std::string getValue;

    /* Ensure field was set */
    try {
      client.hexists(entry, attr_name, [&exists](cpp_redis::reply& reply) {
	if (!reply.is_null()) {
	  exists = reply.as_integer();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return {};
    }
    
    if (!exists) {
      ldpp_dout(dpp, 20) << "RGW Redis Cache: Attribute was not set." << dendl;
      return {};
    }

    /* Retrieve existing value from cache */
    try {
      client.hget(entry, attr_name, [&exists, &attrValue](cpp_redis::reply &reply) {
	if (!reply.is_null()) {
	  attrValue = reply.as_string();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return {};
    }
  } else {
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object is not in cache." << dendl;
    return {};
  }

  return attrValue;
}

int RedisDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attrVal, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  int result = 0;
    
  if (!client.is_connected()) 
    find_client(dpp);
    
  if (key_exists(dpp, key)) {
    /* Every attr set will be treated as new */
    try {
      client.hset(entry, attr_name, attrVal, [&result](cpp_redis::reply& reply) {
	if (!reply.is_null()) {
	  result = reply.as_integer();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object is not in cache." << dendl;
    return -2; 
  }

  return result - 1;
}

static Aio::OpFunc redis_read_op(optional_yield y, boost::redis::connection& conn,
                                 off_t read_ofs, off_t read_len, const std::string& key)
{
  return [y, &conn, read_ofs, read_len, key] (Aio* aio, AioResult& r) mutable {
    using namespace boost::asio;
    async_completion<yield_context, void()> init(y.get_yield_context());
    auto ex = get_associated_executor(init.completion_handler);

    boost::redis::request req;
    req.push("HGET", key, "data");

    // TODO: Make unique pointer once support is added
    auto s = std::make_shared<RedisDriver::redis_response>();
    auto& resp = s->resp;

    conn.async_exec(req, resp, bind_executor(ex, RedisDriver::redis_aio_handler{aio, r, s}));
  };
}

rgw::AioResultList RedisDriver::get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) 
{
  std::string entry = partition_info.location + key;
  rgw_raw_obj r_obj;
  r_obj.oid = key;

  return aio->get(r_obj, redis_read_op(y, conn, ofs, len, entry), cost, id);
}

} } // namespace rgw::cache
