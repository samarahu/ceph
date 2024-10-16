#include <errno.h>
#include <cpp_redis/cpp_redis>
#include "driver/d4n/d4n_cpp_directory.h"
#include <string>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <list>
#include <chrono>
#include <thread>


#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace d4n {

void RGWObjectDirectory::connectClient(){
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

void RGWBlockDirectory::connectClient(){
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

int RGWObjectDirectory::findClient(std::string key){
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

int RGWBlockDirectory::findClient(std::string key){
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
void RGWObjectDirectory::retryFindClient(std::string key, cpp_redis::client *client){
  int delay = 50;
  for ( int retry =0; retry < 3; retry ++){
    //std::this_thread::sleep_for(std::chrono::milliseconds(delay*retry));
    findClient(key, client);
    if ((client->is_connected()))
      return;
  }
}

void RGWBlockDirectory::retryFindClient(std::string key, cpp_redis::client *client){
  int delay = 50;
  for ( int retry =0; retry < 3; retry ++){
    //std::this_thread::sleep_for(std::chrono::milliseconds(delay*retry));
    findClient(key, client);
    if ((client->is_connected()))
      return;
  }
}
*/

std::string RGWObjectDirectory::buildIndex(CacheObjectCpp *ptr){
  return ptr->bucketName + "_" + ptr->objName;
}


std::string RGWBlockDirectory::buildIndex(CacheBlockCpp *ptr){
  return ptr->cacheObj.bucketName + "_" + ptr->cacheObj.objName + "_" + std::to_string(ptr->blockID) + "_" + std::to_string(ptr->size);
}

int RGWObjectDirectory::exist_key(CacheObjectCpp *ptr){

  int result = 0;

  std::string key = buildIndex(ptr);
  ldout(cct,10) << __func__ << "  key " << key << dendl;

  //retryFindClient(key, &client);

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
    std::vector<std::string> keys;
    keys.push_back(key);
    client_conn[client_index].exists(keys, [&result](cpp_redis::reply &reply){
      if (reply.is_integer())
        result = reply.as_integer();
    });
    client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
    ldout(cct,10) << __func__ << ": " << __LINE__ << dendl;
  }
  catch(std::exception &e) {
    result =  0;
    ldout(cct,10) << __func__ << ": " << __LINE__ << ": key " << key <<" result:" <<result <<dendl;
  }
  
  ldout(cct,10) << __func__ << ": " << __LINE__ << ": key " << key <<" result:" <<result <<dendl;
  return result;
}

int RGWBlockDirectory::exist_key(CacheBlockCpp *ptr)
{
  int result = 0;
  std::string key = buildIndex(ptr);

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

int RGWObjectDirectory::set(CacheObjectCpp *ptr, optional_yield y)
{
  //creating the index based on bucket_name, obj_name, and chunk_id
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string key = buildIndex(ptr);
  ldout(cct,10) <<__func__<<": " << __LINE__ << " key is: " << key <<  dendl;
  /*findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }*/

  int client_index = findClient(key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return -1;
  }
  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }

  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string result;
  std::string endpoint;
  std::string local_host = cct->_conf->rgw_local_cache_address;
  int exist = 0;
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;

  std::vector<std::string> keys;
  keys.push_back(key);
  try{
    client_conn[client_index].exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
  }
  catch(std::exception &e) {
    exist = 0;
  }
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
	
  if (!exist) {
    ldout(cct,10) <<__func__<<" not in directory key:  " << key <<  dendl;

    std::vector<std::pair<std::string, std::string>> list;

    list.push_back(std::make_pair("bucketName", ptr->bucketName));
    list.push_back(std::make_pair("objName", ptr->objName));
    list.push_back(std::make_pair("creationTime", ptr->creationTime));
    list.push_back(std::make_pair("dirty", std::to_string(ptr->dirty)));
    list.push_back(std::make_pair("version", ptr->version));
    list.push_back(std::make_pair("size", std::to_string(ptr->size)));
    list.push_back(std::make_pair("in_lsvd", std::to_string(ptr->in_lsvd)));

    for (auto const& host : ptr->hostsList) {
      if (endpoint.empty())
        endpoint = host + "_";
      else
        endpoint = endpoint + host + "_";
    }
    if (!endpoint.empty())
      endpoint.pop_back();
    list.push_back(std::make_pair("objHosts", endpoint));

    for (auto& it : ptr->attrs) {
      list.push_back(std::make_pair(it.first, it.second.to_str()));
    }

    client_conn[client_index].hmset(key, list, [&result](cpp_redis::reply &reply){
      if (!reply.is_null())
 	result = reply.as_string();
    });

    client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
    if (result.find("OK") != std::string::npos)
      ldout(cct,10) <<__func__<<" new key res  " << result <<dendl;
    else
      ldout(cct,10) <<__func__<<" else key res  " << result <<dendl;
  } else { 
    std::string old_val;
    std::vector<std::string> fields;
    fields.push_back("objHosts");
    try {
      client_conn[client_index].hmget(key, fields, [&old_val](cpp_redis::reply &reply){
        if (reply.is_array()){
	  auto arr = reply.as_array();
	  if (!arr[0].is_null())
	    old_val = arr[0].as_string();
	}
      });
      client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
    }
    catch(std::exception &e) {
      return 0;
    }
  
    if (old_val.find(local_host) == std::string::npos){
      std::string hosts = old_val +"_"+ local_host;
      std::vector<std::pair<std::string, std::string>> list;
      list.push_back(std::make_pair("objHosts", hosts));

      client_conn[client_index].hmset(key, list, [&result](cpp_redis::reply &reply){
        if (!reply.is_null())
 	  result = reply.as_string();
        });

      client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
	  
      ldout(cct,10) <<__func__<<" after hmset " << key << " updated hostslist: " << old_val <<dendl;
    }
  }
  return 0;
}

int RGWBlockDirectory::set(CacheBlockCpp *ptr, optional_yield y)
{
  //creating the index based on bucket_name, obj_name, and chunk_id
  std::string key = buildIndex(ptr);
  /*findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }*/

  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " key is: " << key << dendl;

  int client_index = findClient(key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return -1;
  }
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " client_index is: " << client_index << dendl;
  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }

  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;

  std::string result;
  std::string endpoint;
  std::string local_host = cct->_conf->rgw_local_cache_address;
  int exist = 0;
  
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " local host is: " << local_host << dendl;

  std::vector<std::string> keys;
  keys.push_back(key);
  try{
    client_conn[client_index].exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
  }
  catch(std::exception &e) {
    exist = 0;
  }
	
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
  if (!exist) {
    ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << " key does not exist!" << dendl;
    std::vector<std::pair<std::string, std::string>> list;

    list.push_back(std::make_pair("blockID", std::to_string(ptr->blockID)));
    list.push_back(std::make_pair("version", ptr->version));
    list.push_back(std::make_pair("size", std::to_string(ptr->size)));
    list.push_back(std::make_pair("bucketName", ptr->cacheObj.bucketName));
    list.push_back(std::make_pair("objName", ptr->cacheObj.objName));
    list.push_back(std::make_pair("creationTime", ptr->cacheObj.creationTime));
    list.push_back(std::make_pair("dirty", std::to_string(ptr->cacheObj.dirty)));
    list.push_back(std::make_pair("globalWeight", std::to_string(ptr->globalWeight)));

    for (auto const& host : ptr->hostsList) {
      if (endpoint.empty())
        endpoint = host + "_";
      else
        endpoint = endpoint + host + "_";
    }
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;

    if (!endpoint.empty())
      endpoint.pop_back();
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;

    list.push_back(std::make_pair("blockHosts", endpoint));

    client_conn[client_index].hmset(key, list, [&result](cpp_redis::reply &reply){
      if (!reply.is_null())
 	result = reply.as_string();
    });
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;

    client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
    if (result.find("OK") != std::string::npos)
      ldout(cct,10) <<__func__<<" new key res  " << result <<dendl;
    else
      ldout(cct,10) <<__func__<<" else key res  " << result <<dendl;
  } else { 
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
    std::string old_val;
    std::vector<std::string> fields;
    fields.push_back("blockHosts");
    try {
      client_conn[client_index].hmget(key, fields, [&old_val](cpp_redis::reply &reply){
        if (reply.is_array()){
	  auto arr = reply.as_array();
	  if (!arr[0].is_null())
	    old_val = arr[0].as_string();
	}
      });
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
      client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
    }
    catch(std::exception &e) {
      return 0;
    }
  
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
    if (old_val.find(local_host) == std::string::npos){
      std::string hosts = old_val +"_"+ local_host;
      std::vector<std::pair<std::string, std::string>> list;
      list.push_back(std::make_pair("blockHosts", hosts));
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;

      client_conn[client_index].hmset(key, list, [&result](cpp_redis::reply &reply){
        if (!reply.is_null())
 	  result = reply.as_string();
        });
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;

      client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
	  
      ldout(cct,10) <<__func__<<" after hmset " << key << " updated hostslist: " << old_val <<dendl;
    }
  }
  ldout(cct, 20) << "AMIN: " << __func__ << "(): " << __LINE__ << dendl;
  return 0;
}

int RGWObjectDirectory::update_field(CacheObjectCpp *ptr, std::string field, std::string value, optional_yield y)
{
  std::vector<std::pair<std::string, std::string>> list;
  list.push_back(std::make_pair(field, value));

  //creating the index based on bucket_name, obj_name, and chunk_id
  std::string key = buildIndex(ptr);
  /*findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }*/

  int client_index = findClient(key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return -1;
  }
  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }


  std::string result;
  int exist = 0;
  std::vector<std::string> keys;
  keys.push_back(key);
  try{
    client_conn[client_index].exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
  }
  catch(std::exception &e) {
    exist = 0;
  }
	
  if (!exist) 
    return -ENOENT;
  else{
    std::string values;
    std::vector<std::string> fields;
    fields.push_back(field);
    if (field == "objHosts"){ 
      std::string old_val;
      try {
        client_conn[client_index].hmget(key, fields, [&old_val](cpp_redis::reply &reply){
          if (reply.is_array()){
	    auto arr = reply.as_array();
	    if (!arr[0].is_null())
	      old_val = arr[0].as_string();
	  }
        });
        client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
      }
      catch(std::exception &e) {
        return 0;
      }
      if (old_val.find(value) == std::string::npos){
        values = old_val +"_"+ value;
      }
    }
    else
      values = value;

    std::vector<std::pair<std::string, std::string>> list;
    list.push_back(std::make_pair(field, values));

    client_conn[client_index].hmset(key, list, [&result](cpp_redis::reply &reply){
      if (!reply.is_null())
 	result = reply.as_string();
    });
    client_conn[client_index].sync_commit(std::chrono::milliseconds(300));	  
  }

  return 0;

}

int RGWBlockDirectory::remove_host(CacheBlockCpp* block, std::string value, optional_yield y) 
{
  std::string field = "blockHosts";
  std::string key = buildIndex(block);

  int client_index = findClient(key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return -1;
  }
  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }

  int result = 0;
  int exist = 0;
  std::vector<std::string> keys;
  keys.push_back(key);
  try{
    client_conn[client_index].exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
  }
  catch(std::exception &e) {
    exist = 0;
  }
	
  if (!exist) 
    return -ENOENT;
  else{
    std::vector<std::string> fields;
    fields.push_back(field);
      
    std::string old_val;
    try {
      client_conn[client_index].hmget(key, fields, [&old_val](cpp_redis::reply &reply){
        if (reply.is_array()){
	  auto arr = reply.as_array();
	  if (!arr[0].is_null())
	     old_val = arr[0].as_string();
	}
      });
      client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
    }
    catch(std::exception &e) {
      return 0;
    }

    if (old_val.find(value) == std::string::npos){
      return 0; //no host to be removed
    }

    ldout(cct,20) << __func__ << ": " << __LINE__ << " Prev blockHosts value is: " << old_val << dendl;
    size_t host_loc = old_val.find(value);
    old_val.erase(host_loc, value.length()); 
    if (old_val.find('_') == 0)
      old_val.erase(0, 1);
    ldout(cct,20) << __func__ << ": " << __LINE__ << " New blockHosts value is: " << old_val << dendl;


    client_conn[client_index].hset(key, field, old_val, [&result](cpp_redis::reply &reply){
      if (reply.is_integer()){
        result = reply.as_integer();
      }
    });
    client_conn[client_index].sync_commit(std::chrono::milliseconds(300));	  
  }

  if (result > 0) //updated values
    return 0;
  else
    return -1;
}


int RGWBlockDirectory::update_field(CacheBlockCpp *ptr, std::string field, std::string value, optional_yield y)
{
  std::vector<std::pair<std::string, std::string>> list;
  list.push_back(std::make_pair(field, value));

  //creating the index based on bucket_name, obj_name, and chunk_id
  std::string key = buildIndex(ptr);
  /*findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }*/

  int client_index = findClient(key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return -1;
  }
  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }


  std::string result;
  int exist = 0;
  std::vector<std::string> keys;
  keys.push_back(key);
  try{
    client_conn[client_index].exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
  }
  catch(std::exception &e) {
    exist = 0;
  }
	
  if (!exist) 
    return -ENOENT;
  else{
    std::string values;
    std::vector<std::string> fields;
    fields.push_back(field);
    if (field == "blockHosts"){ 
      std::string old_val;
      try {
        client_conn[client_index].hmget(key, fields, [&old_val](cpp_redis::reply &reply){
          if (reply.is_array()){
	    auto arr = reply.as_array();
	    if (!arr[0].is_null())
	      old_val = arr[0].as_string();
	  }
        });
        client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
      }
      catch(std::exception &e) {
        return 0;
      }
      if (old_val.find(value) == std::string::npos){
        values = old_val +"_"+ value;
      }
    }
    else
      values = value;

    std::vector<std::pair<std::string, std::string>> list;
    list.push_back(std::make_pair(field, values));

    client_conn[client_index].hmset(key, list, [&result](cpp_redis::reply &reply){
      if (!reply.is_null())
 	result = reply.as_string();
    });
    client_conn[client_index].sync_commit(std::chrono::milliseconds(300));	  
  }

  return 0;
}

int RGWObjectDirectory::del(CacheObjectCpp *ptr, optional_yield y)
{
  int result = 0;
  std::vector<std::string> keys;
  std::string key = buildIndex(ptr);
  keys.push_back(key);
  /*findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }*/

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
	client_conn[client_index].del(keys, [&result](cpp_redis::reply &reply){
		if  (reply.is_integer())
		{result = reply.as_integer();}
		});
	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));	
	ldout(cct,10) << __func__ << "DONE" << dendl;
	return result-1;
  }
  catch(std::exception &e) {
	ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -1;
  }
}

int RGWBlockDirectory::del(CacheBlockCpp *ptr, optional_yield y)
{
  int result = 0;
  std::vector<std::string> keys;
  std::string key = buildIndex(ptr);
  keys.push_back(key);
  /*findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }*/

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
	client_conn[client_index].del(keys, [&result](cpp_redis::reply &reply){
		if  (reply.is_integer())
		{result = reply.as_integer();}
		});
	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));	
	ldout(cct,10) << __func__ << "DONE" << dendl;
	return result-1;
  }
  catch(std::exception &e) {
	ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -1;
  }
}

int RGWObjectDirectory::get(CacheObjectCpp *ptr, optional_yield y)
{
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string key = buildIndex(ptr);
  ldout(cct,10) <<__func__<<": " << __LINE__ << " key is: " << key <<  dendl;
  /*findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }*/

  int client_index = findClient(key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return -1;
  }
  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }


  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string result;
  int exist = 0;
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;

  std::vector<std::string> keys;
  keys.push_back(key);
    
  client_conn[client_index].exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));

  ldout(cct,10) <<__func__<<": " << __LINE__ <<  ": exist: " << exist << dendl;
	
  if (exist) {
  //if (exist_key(ptr)){
    ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << " exists in directory!" << dendl;
    try{
	std::string obj_name;
	std::string bucket_name;
	std::string creationTime;
	std::string dirty;
	std::string version;
	std::string size;
	std::string in_lsvd;
	std::string objHosts;
	std::string attrs;

	//fields will be filled by the redis hmget functoin
	std::vector<std::string> fields;
	fields.push_back("bucketName");
	fields.push_back("objName");
	fields.push_back("creationTime");
	fields.push_back("dirty");
	fields.push_back("version");
	fields.push_back("size");
	fields.push_back("in_lsvd");
	fields.push_back("objHosts");
	fields.push_back(RGW_ATTR_ACL);

	client_conn[client_index].hmget(key, fields, [&bucket_name, &obj_name, &creationTime, &dirty, &version, &size, &in_lsvd, &objHosts, &attrs](cpp_redis::reply& reply){
	  if (reply.is_array()){
	    auto arr = reply.as_array();
	    if (!arr[0].is_null()){
    	      bucket_name = arr[0].as_string();
	      obj_name = arr[1].as_string();
	      creationTime = arr[2].as_string();
	      dirty = arr[3].as_string();
	      version = arr[4].as_string();
 	      size = arr[5].as_string();
	      in_lsvd = arr[6].as_string();
  	      objHosts = arr[7].as_string();
	      attrs = arr[8].as_string();
	    }
	  }
	});

  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
	  
	std::stringstream sloction(objHosts);
	std::string tmp;
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
	
	ptr->objName = obj_name;
	ptr->bucketName = bucket_name;
	ptr->creationTime = creationTime;
	ptr->dirty = boost::lexical_cast<bool>(dirty);
	//host1_host2_host3_...
	while(getline(sloction, tmp, '_'))
	  ptr->hostsList.push_back(tmp);
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;

	ptr->version = version;
	ptr->size = std::stoull(size);
	ptr->in_lsvd = boost::lexical_cast<bool>(in_lsvd);
	ptr->attrs[RGW_ATTR_ACL] = buffer::list::static_from_string(attrs);
        ldout(cct,10) << __func__ << ": " << __LINE__<< ": objName: "<< obj_name << dendl;
        ldout(cct,10) << __func__ << ": " << __LINE__<< ": version: "<< version << dendl;
        ldout(cct,10) << __func__ << ": " << __LINE__<< ": size: "<< size << dendl;
    }
    catch(std::exception &e) {
      ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -EINVAL;
    }
  }
  else{
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
    return -ENOENT;  
  }
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
  return 0;
}

int RGWBlockDirectory::get(CacheBlockCpp *ptr, optional_yield y)
{
  std::string key = buildIndex(ptr);
  ldout(cct,10) << __func__ << " object in func getValue "<< key << dendl;
  /*
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }*/

  int client_index = findClient(key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return -1;
  }
  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }


  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string result;
  int exist = 0;
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;

  std::vector<std::string> keys;
  keys.push_back(key);
    
  client_conn[client_index].exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));

  ldout(cct,10) <<__func__<<": " << __LINE__ <<  ": exist: " << exist << dendl;
	
  if (exist) {
  //if (exist_key(ptr)){
    try{
	std::string blockID;
	std::string version;
	std::string size;
	std::string globalWeight;
	std::string blockHosts;

	std::string obj_name;
	std::string bucket_name;
	std::string creationTime;
	std::string dirty;

	//fields will be filled by the redis hmget functoin
	std::vector<std::string> fields;

        fields.push_back("blockID");
        fields.push_back("version");
        fields.push_back("size");
	fields.push_back("bucketName");
	fields.push_back("objName");
	fields.push_back("creationTime");
	fields.push_back("dirty");
        fields.push_back("globalWeight");
        fields.push_back("blockHosts");

	client_conn[client_index].hmget(key, fields, [&blockID, &version, &size, &bucket_name, &obj_name, &creationTime, &dirty, &globalWeight, &blockHosts](cpp_redis::reply& reply){
	  if (reply.is_array()){
	    auto arr = reply.as_array();
	    if (!arr[0].is_null()){
	      blockID = arr[0].as_string();
	      version = arr[1].as_string();
 	      size = arr[2].as_string();
    	      bucket_name = arr[3].as_string();
	      obj_name = arr[4].as_string();
	      creationTime  = arr[5].as_string();
	      dirty = arr[6].as_string();
	      globalWeight = arr[7].as_string();
  	      blockHosts = arr[8].as_string();
	    }
	  }
	});

	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
	  
	std::stringstream sloction(blockHosts);
	std::string tmp;
	
	ptr->cacheObj.objName = obj_name;
	ptr->cacheObj.bucketName = bucket_name;
	ptr->cacheObj.creationTime = creationTime;
	ptr->cacheObj.dirty = boost::lexical_cast<bool>(dirty);
	ptr->dirty = boost::lexical_cast<bool>(dirty);

	//host1_host2_host3_...
	while(getline(sloction, tmp, '_'))
	  ptr->hostsList.push_back(tmp);

	ptr->blockID = boost::lexical_cast<uint64_t>(blockID);
	ptr->version = version;
	ptr->size = boost::lexical_cast<uint64_t>(size);
	ptr->globalWeight = boost::lexical_cast<int>(globalWeight);

        ldout(cct,10) << __func__ << ": " << __LINE__<< ": objName: "<< obj_name << dendl;
        ldout(cct,10) << __func__ << ": " << __LINE__<< ": version: "<< version << dendl;
        ldout(cct,10) << __func__ << ": " << __LINE__<< ": size: "<< size << dendl;
    }
    catch(std::exception &e) {
      ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -EINVAL;
    }
  }
  else{
    return -ENOENT;  
  }
  return 0;
}

int RGWObjectDirectory::get_attr(CacheObjectCpp *ptr, const char* name, bufferlist &dest, optional_yield y)
{
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string key = buildIndex(ptr);
  ldout(cct,10) <<__func__<<": " << __LINE__ << " key is: " << key <<  dendl;
  /*findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }*/

  int client_index = findClient(key);
  if (client_index < 0){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Couldn't find the right slot!" << dendl;
    return -1;
  }
  if (!(client_conn[client_index].is_connected())){
    ldout(cct,10) << __func__ << ": " << __LINE__ << " Redis client is not connected!" << dendl;
    return -1;
  }


  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;
  std::string result;
  int exist = 0;
  ldout(cct,10) <<__func__<<": " << __LINE__ <<  dendl;

  std::vector<std::string> keys;
  keys.push_back(key);
    
  client_conn[client_index].exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));

  ldout(cct,10) <<__func__<<": " << __LINE__ <<  ": exist: " << exist << dendl;
	
  if (exist) {
  //if (exist_key(ptr)){
    ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
    try{
	std::string value;
	std::vector<std::string> fields;

        fields.push_back(name);

	client_conn[client_index].hmget(key, fields, [&value](cpp_redis::reply& reply){
	  if (reply.is_array()){
	    auto arr = reply.as_array();
	    if (!arr[0].is_null()){
	      value = arr[0].as_string();
	    }
	  }
	});

	client_conn[client_index].sync_commit(std::chrono::milliseconds(300));
	dest.append(value);
    }
    catch(std::exception &e) {
      ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -EINVAL;
    }
  }
  else{
    ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
    return -ENOENT;  
  }
  ldout(cct,10) << __func__ << ": " << __LINE__<< ": object: "<< key << dendl;
  return 0;
}

} }
