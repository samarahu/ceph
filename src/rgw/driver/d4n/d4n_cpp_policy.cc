#include "d4n_cpp_policy.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"
#include "common/dout.h" 

namespace rgw { namespace d4n {

/*
static const uint16_t crc16tab[256]= {
  0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
  0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
  0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
  0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
  0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
  0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
  0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
  0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
  0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
  0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
  0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
  0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
  0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
  0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
  0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
  0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
  0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
  0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
  0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
  0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
  0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
  0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
  0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
  0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
  0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
  0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
  0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
  0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
  0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
  0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
  0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
  0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

uint16_t crc16(const char *buf, int len) {
  int counter;
  uint16_t crc = 0;
  for (counter = 0; counter < len; counter++)
	crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
  return crc;
}

unsigned int hash_slot(const char *key, int keylen) {
  int s, e; // start-end indexes of { and } 

  // Search the first occurrence of '{'. 
  for (s = 0; s < keylen; s++)
	if (key[s] == '{') break;

  // No '{' ? Hash the whole key. This is the base case. 
  if (s == keylen) return crc16(key,keylen) & 16383;

  // '{' found? Check if we have the corresponding '}'. 
  for (e = s+1; e < keylen; e++)
	if (key[e] == '}') break;

  // No '}' or nothing between {} ? Hash the whole key. 
  if (e == keylen || e == s+1) return crc16(key,keylen) & 16383;

  // If we are here there is both a { and a } on its right. Hash
   * what is in the middle between { and }. 
  return crc16(key+s+1,e-s-1) & 16383;
}
*/

void RGWLFUDAPolicy::connectClient(){
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;

  int dirMasterCount = cct->_conf->rgw_directory_master_count;
  std::string address = cct->_conf->rgw_filter_address;
  std::vector<std::string> host;
  std::vector<size_t> port;
  std::stringstream ss(address);
  for (int i = 0; i < dirMasterCount; i ++){ //host1:port1_host2:port2_....
    while (!ss.eof()) {
      std::string tmp;
      std::getline(ss, tmp, '_');
      host.push_back(tmp.substr(0, tmp.find(":")));
      port.push_back(std::stoul(tmp.substr(tmp.find(":") + 1, tmp.length())));
    }
  }

  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  cpp_redis::connect_state status;
  try {
    for (int i = 0; i < dirMasterCount; i++){
      ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
      client_conn[i].connect(host[i], port[i],
	[&status](const std::string &host, std::size_t port, cpp_redis::connect_state statusC) {
	  if (statusC == cpp_redis::connect_state::dropped) {
	    status = statusC;
	  }
	});
	
      if (status == cpp_redis::connect_state::dropped)
        ldout(cct, 10) << "AMIN:DEBUG client disconnected from " << host[i] << ":" << port[i] << dendl;
    }
  }
  catch(std::exception &e) {
    ldout(cct,10) << __func__ <<"Redis client connected failed!" << dendl;
  }
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
}

int RGWLFUDAPolicy::findClient(std::string key){
  int slot = 0;
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  slot = hash_slot(key.c_str(), key.size());
  int dirMasterCount = cct->_conf->rgw_directory_master_count;
  int slotQuota = 16384/dirMasterCount; 

  std::string address = cct->_conf->rgw_filter_address;
  std::vector<std::string> host;
  std::vector<size_t> port;
  std::stringstream ss(address);
  for (int i = 0; i < dirMasterCount; i ++){ //host1:port1_host2:port2_....
    while (!ss.eof()) {
      std::string tmp;
      std::getline(ss, tmp, '_');
      host.push_back(tmp.substr(0, tmp.find(":")));
      port.push_back(std::stoul(tmp.substr(tmp.find(":") + 1, tmp.length())));
    }
  }

  for (int i = 0; i < dirMasterCount; i++){
    ldout(cct,10) <<__func__<<": " << __LINE__ << ": slot is: " << slot << " slotQuota is: " << slotQuota << dendl;
    if (slot < (slotQuota*(i+1))){
      return i;
    }
  }
  return -1;
}


/*
int RGWLFUDAPolicy::exist_key(CephContext *cct, std::string key)
{
  int result = 0;

  int client_index = findClient(cct, key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return -1;
  }
  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }

  try {
    std::vector<std::string> keys;
    keys.push_back(key);
    client_conn[client_index].exists(keys, [&result](cpp_redis::reply &reply){
      if (reply.is_integer())
	 result = reply.as_integer();
    });
    client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
    ldout(cct,10) << __func__ << " res dir " << result << " key " << key <<  dendl;
  }
  catch(std::exception &e) {
    ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
  }
  
  return result;
}
*/

int RGWLFUDAPolicy::exist_field(std::string key, std::string field)
{
  int result = 0;

  int client_index = findClient(key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return -1;
  }

  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }

  try {
    client_conn[client_index].hexists(key, field, [&result](cpp_redis::reply &reply){
      if (reply.is_integer())
	 result = reply.as_integer();
    });
    client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
  }
  catch(std::exception &e) {
    ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
  }
  
  return result;
}

int RGWLFUDAPolicy::set(std::string key, std::string field, std::string value)
{
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " key is: " << key << dendl;

  int client_index = findClient(key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return -1;
  }
  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }

  int result;

  client_conn[client_index].hset(key, field, value, [&result](cpp_redis::reply &reply){
    if (reply.is_integer())
      result = reply.as_integer();
  });

  client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " RESULT is: " << result << dendl;

  return result;
}

std::string RGWLFUDAPolicy::get(std::string key, std::string field)
{
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " key is: " << key << dendl;

  int client_index = findClient(key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return "-1";
  }
  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return "-1";
  }

  std::string result;

  client_conn[client_index].hget(key, field, [&result](cpp_redis::reply &reply){
    if (!reply.is_null())
      result = reply.as_string();
  });

  client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " result is: " << result << dendl;

  return result;
}



int RGWLFUDAPolicy::init(CephContext *_cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver) {
  driver = _driver;
  cct = _cct;
  int result = 0;
  connectClient();
  dir->init(cct);

  std::string p_name = "lfuda";
  std::string f_name = "age";

  if (exist_field(p_name, f_name) == 0){
    set(p_name, f_name, std::to_string(age));
  }

  f_name = "minLocalWeights_sum";
  result = set(p_name, f_name, std::to_string(weightSum)); // New cache node will always have the minimum average weight 
  f_name = "minLocalWeights_size";
  result += set(p_name, f_name, std::to_string(entries_map.size()));
  f_name = "minLocalWeights_address";
  result += set(p_name, f_name, dpp->get_cct()->_conf->rgw_local_cache_address);
  
  // Spawn lfuda policy thread
  lfuda_t = std::thread(&RGWLFUDAPolicy::redis_sync, this, dpp, null_yield);
  lfuda_t.detach();

  // Spawn write cache cleaning thread
  if (dpp->get_cct()->_conf->d4n_writecache_enabled == true){
    tc = std::thread(&RGWCachePolicy::cleaning, this, dpp);
    tc.detach();
  }

  return result;
}


int RGWLFUDAPolicy::getMinAvgWeight(const DoutPrefixProvider* dpp, int *minAvgWeight, std::string *cache_address, optional_yield y) {

  std::string res1, res2, res3;

  std::string p_name = "lfuda";
  std::string f_name = "minLocalWeights_sum";
  res1 = get(p_name, f_name);

  f_name = "minLocalWeights_size";
  res2 = get(p_name, f_name);

  f_name = "minLocalWeights_address";
  res3 = get(p_name, f_name);

  if (std::stoi(res2) > 0)
    *minAvgWeight =  std::stoi(res1) / std::stoi(res2);
  else
    *minAvgWeight = 0; 
  *cache_address =  res3;
  ldpp_dout(dpp, 10) << "AMIN " << __func__ << "() cache_address is " << cache_address << dendl;
  return 0;
}



int RGWLFUDAPolicy::age_sync(const DoutPrefixProvider* dpp, optional_yield y) {

  ldpp_dout(dpp, 10) << "AMIN " << __func__ << " " << __LINE__ << dendl;
  std::string result;
  std::string p_name = "lfuda";
  std::string f_name = "age";
  result = get(p_name, f_name);
  ldpp_dout(dpp, 10) << "AMIN " << __func__ << " " << __LINE__ << " result: " << result << dendl;

  if (age > std::stoi(result) || result.empty()) { /* Set new maximum age */
  ldpp_dout(dpp, 10) << "AMIN " << __func__ << " " << __LINE__ << dendl;
    if (set(p_name, f_name, std::to_string(age)) == 0){
  ldpp_dout(dpp, 10) << "AMIN " << __func__ << " " << __LINE__ << dendl;
      return 0;
    }
    else
      return -1;
  } else {
  ldpp_dout(dpp, 20) << "AMIN " << __func__ << " " << __LINE__ << dendl;
    age = std::stoi(result);
  ldpp_dout(dpp, 20) << "AMIN " << __func__ << " " << __LINE__ << dendl;
    return 0;
  }
}

int RGWLFUDAPolicy::local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  int result1 = 0, flag = 0; 
  int result2 = 0; 
   
  ldpp_dout(dpp, 20) << "AMIN " << __func__ << " " << __LINE__ << dendl;
  std::string p_name = "lfuda";
  std::string f_name;

  if (fabs(weightSum - postedSum) > (postedSum * 0.1)) {
    std::string resp0, resp1;

    f_name = "minLocalWeights_sum";
    resp0 = get(p_name, f_name);

    f_name = "minLocalWeights_size";
    resp1 = get(p_name, f_name);
    ldpp_dout(dpp, 20) << "AMIN " << __func__ << " " << __LINE__ << dendl;
	
    float minAvgWeight = std::stof(resp0) / std::stof(resp1);

    if ((static_cast<float>(weightSum) / static_cast<float>(entries_map.size())) < minAvgWeight) { /* Set new minimum weight */
      ldpp_dout(dpp, 20) << "AMIN " << __func__ << " " << __LINE__ << dendl;
      flag = 1;
      p_name = "lfuda";
      f_name = "minLocalWeights_sum";
      result1 = set(p_name, f_name, std::to_string(weightSum));

      f_name = "minLocalWeights_size";
      result1 += set(p_name, f_name, std::to_string(entries_map.size()));

      f_name = "minLocalWeights_address";
      result1 += set(p_name, f_name, dpp->get_cct()->_conf->rgw_local_cache_address);

    } else {
      weightSum = std::stoi(resp0);
      postedSum = std::stoi(resp0);
    }
  }

  ldpp_dout(dpp, 20) << "AMIN " << __func__ << " " << __LINE__ << dendl;
  p_name = dpp->get_cct()->_conf->rgw_local_cache_address;
  f_name = "avgLocalWeight_sum";  
  result2 = set(p_name, f_name, std::to_string(weightSum));

  f_name = "avgLocalWeight_size";
  result2 += set(p_name, f_name, std::to_string(entries_map.size()));
  
  ldpp_dout(dpp, 20) << "AMIN " << __func__ << " " << __LINE__ << " flag is: " << flag << dendl;
  ldpp_dout(dpp, 20) << "AMIN " << __func__ << " " << __LINE__ << " result1 is: " << result1 << dendl;
  ldpp_dout(dpp, 20) << "AMIN " << __func__ << " " << __LINE__ << " result2 is: " << result2 << dendl;
  if (flag == 0)
    return result2;
  else
    return result1+result2;
}

void RGWLFUDAPolicy::redis_sync(const DoutPrefixProvider* dpp, optional_yield y) {

  int interval = dpp->get_cct()->_conf->rgw_lfuda_sync_frequency;
  while(true){
    /* Update age */
    if (int ret = age_sync(dpp, y) < 0) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): " << __LINE__ << " ERROR: ret=" << ret << dendl;
      return;
    }
    
    /* Update minimum local weight sum */
    if (int ret = local_weight_sync(dpp, y) < 0) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): " << __LINE__ << " ERROR: ret=" << ret << dendl;
      return;
    }

    std::this_thread::sleep_for(std::chrono::seconds(interval));
  }
}

CacheBlockCpp* RGWLFUDAPolicy::get_victim_block(const DoutPrefixProvider* dpp, optional_yield y) {
  const std::lock_guard l(lfuda_lock);
  if (entries_heap.empty())
    return nullptr;

  /* Get victim cache block */
  std::string key = entries_heap.top()->key;
  
  ldpp_dout(dpp, 10) << "AMIN " << __func__ << "() key is: " << key << dendl;

  CacheBlockCpp* victim = new CacheBlockCpp();

  victim->cacheObj.bucketName = key.substr(0, key.find('_')); 
  ldpp_dout(dpp, 10) << "AMIN " << __func__ << "() bucket is: " << victim->cacheObj.bucketName << dendl;
  key.erase(0, key.find('_') + 1); //bucket
  key.erase(0, key.find('_') + 1); //version
  victim->cacheObj.objName = key.substr(0, key.find('_'));
  ldpp_dout(dpp, 10) << "AMIN " << __func__ << "() object is: " << victim->cacheObj.objName << dendl;
  victim->blockID = entries_heap.top()->offset;
  victim->size = entries_heap.top()->len;
  ldpp_dout(dpp, 10) << "AMIN " << __func__ << "() blockID is: " << victim->blockID << dendl;
  ldpp_dout(dpp, 10) << "AMIN " << __func__ << "() size is: " << victim->size << dendl;

  if (dir->get(victim, y) < 0) {
    return nullptr;
  }

  ldpp_dout(dpp, 10) << "AMIN " << __func__ << "() getting block from directorty was successful. version is: " << victim->version << dendl;
  return victim;
}

int RGWLFUDAPolicy::exist_key(std::string key) {
  const std::lock_guard l(lfuda_lock);
  if (entries_map.count(key) != 0) {
    return true;
  }

  return false;
}

int RGWLFUDAPolicy::sendRemote(const DoutPrefixProvider* dpp, CacheBlockCpp *victim, std::string remoteCacheAddress, std::string key, bufferlist* out_bl, optional_yield y)
{
  bufferlist in_bl;
  rgw::sal::RGWRemoteD4NGetCB cb(&in_bl);
  std::string bucketName = victim->cacheObj.bucketName;
 
  RGWAccessKey accessKey;
  std::string findKey;
  
  if (key[0] == 'D') {
    findKey = key.substr(2, key.length());
  } else {
    findKey = key;
  }
  
  auto it = entries_map.find(findKey);
  if (it == entries_map.end()) {
    return -ENOENT;
  }
  auto user = it->second->user;
  std::unique_ptr<rgw::sal::User> c_user = driver->get_user(user);
  int ret = c_user->load_user(dpp, y);
  if (ret < 0) {
    return -EPERM;
  }

  if (c_user->get_info().access_keys.empty()) {
    return -EINVAL;
  }

  accessKey.id = c_user->get_info().access_keys.begin()->second.id;
  accessKey.key = c_user->get_info().access_keys.begin()->second.key;

  HostStyle host_style = PathStyle;
  std::map<std::string, std::string> extra_headers;                                                            

  auto sender = new RGWRESTStreamRWRequest(dpp->get_cct(), "PUT", remoteCacheAddress, &cb, NULL, NULL, "", host_style);

  ret = sender->send_request(dpp, &accessKey, extra_headers, "admin/remoted4n/"+bucketName+"/"+key, nullptr, out_bl);                 
  if (ret < 0) {                                                                                      
    delete sender;                                                                                       
    return ret;                                                                                       
  }                                                                                                   
  
  ret = sender->complete_request(y);                                                            
  if (ret < 0){
    delete sender;                                                                                   
    return ret;                                                                                   
  }

  return 0;
}	

int RGWLFUDAPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) {
  uint64_t freeSpace = cacheDriver->get_free_space(dpp);

  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " free space is " << freeSpace << dendl;
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " size is " << size << dendl;
  while (freeSpace < size) { // TODO: Think about parallel reads and writes; can this turn into an infinite loop? 
    CacheBlockCpp* victim = get_victim_block(dpp, y);

    if (victim == nullptr) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Could not retrieve victim block." << dendl;
      delete victim;
      return -ENOENT;
    }

    const std::lock_guard l(lfuda_lock);
    std::string key = entries_heap.top()->key;
    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " key is " << key << dendl;
    auto it = entries_map.find(key);
    if (it == entries_map.end()) {
      delete victim;
      return -ENOENT;
    }
    else{//victim block is getting read, no suitable block to evict
      if (it->second->read_flag == 1){
 	/*
	//it->second->localWeight += victim->globalWeight;
        (*it->second->handle)->localWeight += 1; //it->second->localWeight;
	entries_heap.increase(it->second->handle);
        ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " localWeight: " << (*(entries_map.find(key))->second->handle)->localWeight << dendl;
        ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " top is: " << entries_heap.top()->key << dendl;
        delete victim;
	continue;
	*/
        delete victim;
        return -ENOENT;
      }
    }

    //int avgWeight = weightSum / entries_map.size();
    int avgWeight;
    std::string remoteCacheAddress;
    if (getMinAvgWeight(dpp, &avgWeight, &remoteCacheAddress, y) < 0){
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Could not retrieve min average weight." << dendl;
      delete victim;
      return -ENOENT;
    }

    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " remote cache address is " << remoteCacheAddress << dendl;

    if (victim->hostsList.size() == 1 && victim->hostsList[0] == dpp->get_cct()->_conf->rgw_local_cache_address) { /* Last copy */
      ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
      if (victim->globalWeight) {
	it->second->localWeight += victim->globalWeight;
        (*it->second->handle)->localWeight = it->second->localWeight;
	entries_heap.increase(it->second->handle);

	if (int ret = cacheDriver->set_attr(dpp, key, "user.rgw.localWeight", std::to_string(it->second->localWeight), y) < 0) { 
	  delete victim;
	  return ret;
        }

	victim->globalWeight = 0;
	if (int ret = dir->update_field(victim, "globalWeight", std::to_string(victim->globalWeight), y) < 0) {
	  delete victim;
	  return ret;
        }
      }

      if (it->second->localWeight > avgWeight) {
        ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
	// TODO: push victim block to remote cache
	// add remote cache host to host list
	bufferlist out_bl;
        rgw::sal::Attrs obj_attrs;

	std::string remoteKey = key;
	if (it->second->dirty == true)
	  remoteKey = "D_"+key; //TODO: AMIN: we should not delete dirty data. it should be cleaned first.
	
	/*
        ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " size is " << size << dendl;
        ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " remote cache address is " << remoteCacheAddress << dendl;
    	cacheDriver->get(dpp, key, 0, it->second->len, out_bl, obj_attrs, y);
	if (int ret = sendRemote(dpp, victim, remoteCacheAddress, remoteKey, &out_bl, y) < 0){
          ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): " << __LINE__ << " sending to remote has failed: " << remoteCacheAddress << dendl;
          delete victim;
          return ret;
        }
        ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " sending to remote is done: " << remoteCacheAddress << dendl;
        if (int ret = dir->update_field(victim, "blockHosts", remoteCacheAddress, y) < 0){
          ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): " << __LINE__ << " updating directory has failed!" << dendl;
          delete victim;
          return ret;
        }
        ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << dendl;
	*/
      }
      ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << dendl;
    }
    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << dendl;

    victim->globalWeight += it->second->localWeight;
    if (int ret = dir->update_field(victim, "globalWeight", std::to_string(victim->globalWeight), y) < 0) {
      delete victim;
      return ret;
    }

    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << dendl;
    if (int ret = dir->remove_host(victim, dpp->get_cct()->_conf->rgw_local_cache_address, y) < 0) {
      delete victim;
      return ret;
    }


/* FIXME: AMIN Remove, just for testing */
    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << dendl;
  CacheBlockCpp* victim1 = new CacheBlockCpp();
    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << dendl;
  victim1->cacheObj.bucketName = victim->cacheObj.bucketName;
  victim1->cacheObj.objName = victim->cacheObj.objName;
  victim1->blockID = victim->blockID;
  victim1->size = victim->size;
    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << dendl;

  if (int ret = dir->get(victim1, y) < 0) {
    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " FAILED!" << dendl;
    delete victim1;
    return ret;
  }

  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << " HOST SIZE is : " << victim1->hostsList.size() << dendl;
  if (victim1->hostsList.size() > 0){
    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " TEST: KEY: " << victim1->blockID << " hosts: " << victim1->hostsList[0] << dendl;
  }
  else{
    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " there is NO HOST!" << dendl;
  }
  
  delete victim1;
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << dendl;

/* END FIXME */




    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << dendl;
    delete victim;

    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << dendl;
    if (int ret = cacheDriver->delete_data(dpp, key, y) < 0) 
      return ret;

    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Block " << key << " has been evicted." << dendl;

    weightSum = (avgWeight * entries_map.size()) - it->second->localWeight;

    age = std::max(it->second->localWeight, age);

    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__  << dendl;
    erase(dpp, key, y);
    freeSpace = cacheDriver->get_free_space(dpp);
  }
  
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " free space is: " << freeSpace << dendl;
  return 0;
}

void RGWLFUDAPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, const rgw_user user, optional_yield y)
{
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ <<  dendl;
  using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
  const std::lock_guard l(lfuda_lock);
  int localWeight = age;
  auto entry = find_entry(key);
  if (entry != nullptr) { 
    localWeight = entry->localWeight + age;
  }  
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ <<  dendl;

  erase(dpp, key, y);
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ <<  dendl;
 
  LFUDAEntry *e = new LFUDAEntry(key, offset, len, version, dirty, creationTime, user, localWeight);
  if (offset != 0 || len != 0){ //not a head object 
    handle_type handle = entries_heap.push(e);
    e->set_handle(handle);
  }
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ <<  dendl;
  entries_map.emplace(key, e);
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ <<  dendl;

  std::string oid_in_cache = key;
  if (dirty == true)
    oid_in_cache = "D_"+key;

  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ <<  dendl;
  if (cacheDriver->set_attr(dpp, oid_in_cache, "user.rgw.localWeight", std::to_string(localWeight), y) < 0) 
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed." << dendl;
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ <<  dendl;

  weightSum += ((localWeight < 0) ? 0 : localWeight);
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ <<  dendl;
}

void RGWLFUDAPolicy::updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, optional_yield y)
{
  eraseObj(dpp, key, y);
  
  const std::lock_guard l(lfuda_lock);
  LFUDAObjEntry *e = new LFUDAObjEntry(key, version, dirty, size, creationTime, user, etag);
  o_entries_map.emplace(key, e);
}


bool RGWLFUDAPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }

  weightSum -= ((p->second->localWeight < 0) ? 0 : p->second->localWeight);

  if (p->second->offset != 0 || p->second->len != 0){ //not a head object
    entries_heap.erase(p->second->handle);
  }
  entries_map.erase(p);

  return true;
}

bool RGWLFUDAPolicy::eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lfuda_lock);
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }

  o_entries_map.erase(p);

  return true;
}

void RGWLFUDAPolicy::set_read_flag(const DoutPrefixProvider* dpp, std::string key, int value)
{
  auto it = entries_map.find(key);
  if (it == entries_map.end()) {
    ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " key: " << key  << " does not exist!" << dendl;
    return;
  }
  
  it->second->read_flag = value;
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " key: " << key  << " read flag is set to: " << value << dendl;
  return;
}

int RGWLFUDAPolicy::get_read_flag(const DoutPrefixProvider* dpp, std::string key)
{
  auto it = entries_map.find(key);
  if (it == entries_map.end()) {
    return -1;
  }
  
  return it->second->read_flag;
}


void RGWLFUDAPolicy::cleaning(const DoutPrefixProvider* dpp)
{
  const int interval = dpp->get_cct()->_conf->rgw_d4n_cache_cleaning_interval;
  while(true){
    ldpp_dout(dpp, 20) << __func__ << " : " << " Cache cleaning!" << dendl;
    std::string name = ""; 
    std::string b_name = ""; 
    std::string key = ""; 
    uint64_t len = 0;
    rgw::sal::Attrs obj_attrs;
    int count = 0;

    for (auto it = o_entries_map.begin(); it != o_entries_map.end(); it++){
      if ((it->second->dirty == true) && (std::difftime(time(NULL), it->second->creationTime) > interval / 1000)){ //if block is dirty and written more than interval seconds ago
	name = it->first;
	rgw_user c_rgw_user = it->second->user;

	size_t pos = 0;
	std::string delimiter = "_";
	while ((pos = name.find(delimiter)) != std::string::npos) {
	  if (count == 0){
	    b_name = name.substr(0, pos);
    	    name.erase(0, pos + delimiter.length());
	  }
	  count ++;
	}
	key = name;

	//writing data to the backend
	//we need to create an atomic_writer
 	rgw_obj_key c_obj_key = rgw_obj_key(key); 		
	std::unique_ptr<rgw::sal::User> c_user = driver->get_user(c_rgw_user);

	std::unique_ptr<rgw::sal::Bucket> c_bucket;
        rgw_bucket c_rgw_bucket = rgw_bucket(c_rgw_user.tenant, b_name, "");

	RGWBucketInfo c_bucketinfo;
	c_bucketinfo.bucket = c_rgw_bucket;
	c_bucketinfo.owner = c_rgw_user;
	
	
    	int ret = driver->load_bucket(dpp, c_rgw_bucket, &c_bucket, null_yield);
	if (ret < 0) {
      	  ldpp_dout(dpp, 10) << __func__ << "(): load_bucket() returned ret=" << ret << dendl;
      	  break;
        }

	std::unique_ptr<rgw::sal::Object> c_obj = c_bucket->get_object(c_obj_key);

	std::unique_ptr<rgw::sal::Writer> processor =  driver->get_atomic_writer(dpp,
				  null_yield,
				  c_obj.get(),
				  c_user->get_id(),
				  NULL,
				  0,
				  "");

  	int op_ret = processor->prepare(null_yield);
  	if (op_ret < 0) {
    	  ldpp_dout(dpp, 20) << "processor->prepare() returned ret=" << op_ret << dendl;
    	  break;
  	}

	std::string prefix = b_name+"_"+key;
	off_t lst = it->second->size;
  	off_t fst = 0;
  	off_t ofs = 0;

	
  	rgw::sal::DataProcessor *filter = processor.get();
	do {
    	  ceph::bufferlist data;
    	  if (fst >= lst){
      	    break;
    	  }
    	  off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
	  off_t cur_len = cur_size - fst;
    	  std::string oid_in_cache = "D_" + prefix + "_" + std::to_string(fst) + "_" + std::to_string(cur_len);
    	  std::string new_oid_in_cache = prefix + "_" + std::to_string(fst) + "_" + std::to_string(cur_len);
    	  cacheDriver->get(dpp, oid_in_cache, 0, cur_len, data, obj_attrs, null_yield);
    	  len = data.length();
    	  fst += len;

    	  if (len == 0) {
      	    break;
   	  }

    	  ceph::bufferlist dataCP = data;
    	  op_ret = filter->process(std::move(data), ofs);
    	  if (op_ret < 0) {
      	    ldpp_dout(dpp, 20) << "processor->process() returned ret="
          	<< op_ret << dendl;
      	    return;
    	  }
	  

  	  rgw::d4n::CacheBlockCpp block;
    	  block.cacheObj.bucketName = c_obj->get_bucket()->get_name();
    	  block.cacheObj.objName = c_obj->get_key().get_oid();
      	  block.size = len;
     	  block.blockID = ofs;
	  op_ret = dir->update_field(&block, "dirty", "false", null_yield); 
    	  if (op_ret < 0) {
      	    ldpp_dout(dpp, 20) << "updating dirty flag in Block directory failed!" << dendl;
      	    return;
    	  }

	  //FIXME: AMIN: this is for testing remote cache.
	  //comment or remove it afterwards
	  
          int ret = -1;
	  std::string remoteCacheAddress = dpp->get_cct()->_conf->rgw_remote_cache_address;
	  if (ret = sendRemote(dpp, &block, remoteCacheAddress, oid_in_cache, &dataCP, y) < 0){
      	    ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << ": sendRemote returned ret=" << ret << dendl;
	  } else {
            if ((ret = dir->update_field(&block, "blockHosts", remoteCacheAddress, null_yield) < 0)) {
	      ldpp_dout(dpp, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << ": update_field returned ret=" << ret << dendl;
            }
          }
         
    	  cacheDriver->rename(dpp, oid_in_cache, new_oid_in_cache, null_yield);

    	  ofs += len;
  	} while (len > 0);

  	op_ret = filter->process({}, ofs);
	
  	const req_context rctx{dpp, null_yield, nullptr};
	ceph::real_time mtime = ceph::real_clock::from_time_t(it->second->creationTime);
        op_ret = processor->complete(lst, it->second->etag, &mtime, ceph::real_clock::from_time_t(it->second->creationTime), obj_attrs,
                               ceph::real_time(), nullptr, nullptr,
                               nullptr, nullptr, nullptr,
                               rctx, rgw::sal::FLAG_LOG_OP);

	//data is clean now, updating in-memory metadata
	it->second->dirty = false;
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(interval));
  }
}


int RGWLRUPolicy::exist_key(std::string key)
{
  const std::lock_guard l(lru_lock);
  if (entries_map.count(key) != 0) {
      return true;
    }
    return false;
}

int RGWLRUPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  uint64_t freeSpace = cacheDriver->get_free_space(dpp);

  while (freeSpace < size) {
    auto p = entries_lru_list.front();
    entries_map.erase(entries_map.find(p.key));
    entries_lru_list.pop_front_and_dispose(Entry_delete_disposer());
    auto ret = cacheDriver->delete_data(dpp, p.key, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << __func__ << "(): Failed to delete data from the cache backend: " << ret << dendl;
      return ret;
    }

    freeSpace = cacheDriver->get_free_space(dpp);
  }

  return 0;
}

void RGWLRUPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, time_t creationTime, const rgw_user user, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  _erase(dpp, key, y);
  Entry *e = new Entry(key, offset, len, version, dirty, creationTime, user);
  entries_lru_list.push_back(*e);
  entries_map.emplace(key, e);
}

void RGWLRUPolicy::updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, time_t creationTime, const rgw_user user, std::string& etag, optional_yield y)
{
  eraseObj(dpp, key, y);
  const std::lock_guard l(lru_lock);
  ObjEntry *e = new ObjEntry(key, version, dirty, size, creationTime, user, etag);
  o_entries_map.emplace(key, e);
  return;
}


bool RGWLRUPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  return _erase(dpp, key, y);
}

bool RGWLRUPolicy::_erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }
  entries_map.erase(p);
  entries_lru_list.erase_and_dispose(entries_lru_list.iterator_to(*(p->second)), Entry_delete_disposer());
  return true;
}

bool RGWLRUPolicy::eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }
  o_entries_map.erase(p);
  return true;
}

} } // namespace rgw::d4n
