#include <stdlib.h>
#include <sys/types.h>
#include <sstream>
#include "rgw_common.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>
#include <vector>
#include <list>
#include <cstdint>
#include <boost/lexical_cast.hpp>

namespace rgw { namespace d4n {

struct CacheObjectCpp {
  std::string objName; /* S3 object name */
  std::string bucketName; /* S3 bucket name */
  std::string creationTime; /* Creation time of the S3 Object */
  bool dirty;
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of object locations for multiple backends */
  std::string version;
  uint64_t size; /* Object size in bytes */
  bool in_lsvd = false; /* is it in LSVD cache? */
  rgw::sal::Attrs attrs; /* List of object attributes */
};

struct CacheBlockCpp {
  CacheObjectCpp cacheObj;
  uint64_t blockID;
  std::string version;
  bool dirty;
  uint64_t size; /* Block size in bytes */
  bool in_lsvd = false; /* is it in LSVD cache? */
  int globalWeight = 0; /* LFUDA policy variable */
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of block locations */
};



class RGWDirectory{
public:
	RGWDirectory(std::shared_ptr<cpp_redis::client[]>& conn): client_conn(conn) {}
	virtual ~RGWDirectory(){}
	CephContext *cct;
        std::shared_ptr<cpp_redis::client[]> client_conn;

private:

};

class RGWObjectDirectory: public RGWDirectory {
public:

	RGWObjectDirectory(std::shared_ptr<cpp_redis::client[]>& conn): RGWDirectory(conn) {}
	void init(CephContext *_cct) {
  	  cct = _cct;
	  connectClient();
	}
	virtual ~RGWObjectDirectory() {}

	//void retryFindClient(std::string key, cpp_redis::client *client);
	void connectClient();
	int findClient(std::string key);
	int set(CacheObjectCpp *ptr, optional_yield y);
	int get(CacheObjectCpp *ptr, optional_yield y);
        int copy(CacheObjectCpp* object, std::string copyName, std::string copyBucketName, optional_yield y){return 0;} //TODO implement this
	int del(CacheObjectCpp *ptr, optional_yield y);
	int update_field(CacheObjectCpp *ptr, std::string field, std::string value, optional_yield y);
	int get_attr(CacheObjectCpp *ptr, const char* name, bufferlist &dest, optional_yield y);
private:
	int exist_key(CacheObjectCpp *ptr);
	std::string buildIndex(CacheObjectCpp *ptr);
};



class RGWBlockDirectory: public RGWDirectory {
public:

	RGWBlockDirectory(std::shared_ptr<cpp_redis::client[]>& conn): RGWDirectory(conn) {}
	void init(CephContext *_cct) {
	  cct = _cct;
	  connectClient();
	}
	virtual ~RGWBlockDirectory() {}

	//void retryFindClient(std::string key, cpp_redis::client *client);
	void connectClient();
	int findClient(std::string key);
	int set(CacheBlockCpp *ptr, optional_yield y);
	int get(CacheBlockCpp *ptr, optional_yield y);
        int copy(CacheBlockCpp* ptr, std::string copyName, std::string copyBucketName, optional_yield y){return 0;} //TODO: implement this
	int del(CacheBlockCpp *ptr, optional_yield y);
	int update_field(CacheBlockCpp *ptr, std::string field, std::string value, optional_yield y);
private:
	int exist_key(CacheBlockCpp *ptr);
	std::string buildIndex(CacheBlockCpp *ptr);
};

} } 
