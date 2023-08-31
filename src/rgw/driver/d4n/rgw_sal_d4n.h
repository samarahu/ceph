// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_sal_filter.h"
#include "rgw_sal.h"
#include "rgw_oidc_provider.h"
#include "rgw_role.h"
#include "common/dout.h" 
#include "rgw_aio_throttle.h"

#include "rgw_redis_driver.h"
#include "driver/d4n/d4n_directory.h"
#include "driver/d4n/d4n_policy.h"

#include <boost/asio/io_context.hpp>
#include <boost/redis/connection.hpp>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace sal {

class D4NFilterDriver : public FilterDriver {
  private:
    rgw::cache::CacheDriver* cacheDriver;
    rgw::d4n::ObjectDirectory* objDir;
    rgw::d4n::BlockDirectory* blockDir;
    rgw::d4n::CacheBlock* cacheBlock;
    rgw::d4n::PolicyDriver* policyDriver;

  public:
    D4NFilterDriver(Driver* _next, boost::asio::io_context& io_context) : FilterDriver(_next)
    {
      rgw::cache::Partition partition_info;
      partition_info.location = "RedisCache"; // figure out how to fill rest of partition information -Sam

      cacheDriver = new rgw::cache::RedisDriver(io_context, partition_info); // change later -Sam
      objDir = new rgw::d4n::ObjectDirectory(io_context);
      blockDir = new rgw::d4n::BlockDirectory(io_context);
      cacheBlock = new rgw::d4n::CacheBlock();
      policyDriver = new rgw::d4n::PolicyDriver(io_context, "lfuda");
    }
    virtual ~D4NFilterDriver() {
      delete cacheDriver;
      delete objDir; 
      delete blockDir; 
      delete cacheBlock;
      delete policyDriver;
    }

    virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
    virtual std::unique_ptr<User> get_user(const rgw_user& u) override;

    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;

    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;
    rgw::cache::CacheDriver* get_cache_driver() { return cacheDriver; }
    rgw::d4n::ObjectDirectory* get_obj_dir() { return objDir; }
    rgw::d4n::BlockDirectory* get_block_dir() { return blockDir; }
    rgw::d4n::CacheBlock* get_cache_block() { return cacheBlock; }
    rgw::d4n::PolicyDriver* get_policy_driver() { return policyDriver; }
};

class D4NFilterUser : public FilterUser {
  private:
    D4NFilterDriver* driver;

  public:
    D4NFilterUser(std::unique_ptr<User> _next, D4NFilterDriver* _driver) : 
      FilterUser(std::move(_next)),
      driver(_driver) {}
    virtual ~D4NFilterUser() = default;

    virtual int create_bucket(const DoutPrefixProvider* dpp,
                            const rgw_bucket& b,
                            const std::string& zonegroup_id,
                            rgw_placement_rule& placement_rule,
                            std::string& swift_ver_location,
                            const RGWQuotaInfo* pquota_info,
                            const RGWAccessControlPolicy& policy,
                            Attrs& attrs,
                            RGWBucketInfo& info,
                            obj_version& ep_objv,
                            bool exclusive,
                            bool obj_lock_enabled,
                            bool* existed,
                            req_info& req_info,
                            std::unique_ptr<Bucket>* bucket,
                            optional_yield y) override;
};

class D4NFilterBucket : public FilterBucket {
  private:
    D4NFilterDriver* driver;

  public:
    D4NFilterBucket(std::unique_ptr<Bucket> _next, User* _user, D4NFilterDriver* _driver) :
      FilterBucket(std::move(_next), _user), 
      driver(_driver) {}
    virtual ~D4NFilterBucket() = default;
   
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
};

class D4NFilterObject : public FilterObject {
  private:
    D4NFilterDriver* driver;

  public:
    struct D4NFilterReadOp : FilterReadOp {
      public:
	class D4NFilterGetCB: public RGWGetDataCB {
	  private:
	    D4NFilterDriver* filter; // don't need -Sam ?
	    std::string oid;
	    D4NFilterObject* source;
	    RGWGetDataCB* client_cb;
	    uint64_t ofs = 0, len = 0;
	    bufferlist bl_rem;
	    bool last_part{false};
	    D3nGetObjData d3n_get_data; // should make d4n version? -Sam
	    bool write_to_cache{true};

	  public:
	    D4NFilterGetCB(D4NFilterDriver* _filter, std::string& _oid, 
		D4NFilterObject* _source) : filter(_filter), 
					    oid(_oid), 
					    source(_source) {}

	    const DoutPrefixProvider* save_dpp;
	    optional_yield* save_y;

	    int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override;
	    void set_client_cb(RGWGetDataCB* client_cb) { this->client_cb = client_cb;}
	    void set_ofs(uint64_t ofs) { this->ofs = ofs; }
	    int flush_last_part(const DoutPrefixProvider* dpp, optional_yield y);
	    void bypass_cache_write() { this->write_to_cache = false; }
	};

	D4NFilterObject* source;

	D4NFilterReadOp(std::unique_ptr<ReadOp> _next, D4NFilterObject* _source) : FilterReadOp(std::move(_next)),
										   source(_source) 
        {
	  //std::string oid = source->get_bucket()->get_marker() + "_" + source->get_key().get_oid(); // How to use in redis driver? -Sam
          std::string oid = source->get_oid();
          cb = std::make_unique<D4NFilterGetCB>(source->driver, oid, source); 
	}
	virtual ~D4NFilterReadOp() = default;

	virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
	virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
	  RGWGetDataCB* cb, optional_yield y) override;

      private:
	RGWGetDataCB* client_cb;
	std::unique_ptr<D4NFilterGetCB> cb;
        std::unique_ptr<rgw::Aio> aio;
	uint64_t offset = 0; // next offset to write to client
        rgw::AioResultList completed; // completed read results, sorted by offset

	int flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results);
	void cancel();
	int drain(const DoutPrefixProvider* dpp);
    };

    struct D4NFilterDeleteOp : FilterDeleteOp {
      D4NFilterObject* source;

      D4NFilterDeleteOp(std::unique_ptr<DeleteOp> _next, D4NFilterObject* _source) : FilterDeleteOp(std::move(_next)),
										     source(_source) {}
      virtual ~D4NFilterDeleteOp() = default;

      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) override;
    };

    D4NFilterObject(std::unique_ptr<Object> _next, D4NFilterDriver* _driver) : FilterObject(std::move(_next)),
									      driver(_driver) {}
    D4NFilterObject(std::unique_ptr<Object> _next, Bucket* _bucket, D4NFilterDriver* _driver) : FilterObject(std::move(_next), _bucket),
											       driver(_driver) {}
    D4NFilterObject(D4NFilterObject& _o, D4NFilterDriver* _driver) : FilterObject(_o),
								    driver(_driver) {}
    virtual ~D4NFilterObject() = default;

    virtual int copy_object(User* user,
               req_info* info, const rgw_zone_id& source_zone,
               rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
               rgw::sal::Bucket* src_bucket,
               const rgw_placement_rule& dest_placement,
               ceph::real_time* src_mtime, ceph::real_time* mtime,
               const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
               bool high_precision_time,
               const char* if_match, const char* if_nomatch,
               AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
               RGWObjCategory category, uint64_t olh_epoch,
               boost::optional<ceph::real_time> delete_at,
               std::string* version_id, std::string* tag, std::string* etag,
               void (*progress_cb)(off_t, void *), void* progress_data,
               const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual const std::string &get_name() const override { return next->get_name(); }
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y) override;
    virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                            rgw_obj* target_obj = NULL) override;
    virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp) override;
    virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y) override;

    virtual std::unique_ptr<ReadOp> get_read_op() override;
    virtual std::unique_ptr<DeleteOp> get_delete_op() override;
};

class D4NFilterWriter : public FilterWriter {
  private:
    D4NFilterDriver* driver; 
    const DoutPrefixProvider* save_dpp;
    bool atomic;
    optional_yield y;

  public:
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterDriver* _driver, Object* _obj, 
	const DoutPrefixProvider* _dpp, optional_yield _y) : FilterWriter(std::move(_next), _obj),
							     driver(_driver),
							     save_dpp(_dpp), atomic(false), y(_y) {}
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterDriver* _driver, Object* _obj, 
	const DoutPrefixProvider* _dpp, bool _atomic, optional_yield _y) : FilterWriter(std::move(_next), _obj),
									   driver(_driver),
									   save_dpp(_dpp), atomic(_atomic), y(_y) {}
    virtual ~D4NFilterWriter() = default;

    virtual int prepare(optional_yield y);
    virtual int process(bufferlist&& data, uint64_t offset) override;
    virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y) override;
   bool is_atomic() { return atomic; };
   const DoutPrefixProvider* dpp() { return save_dpp; } 
};

} } // namespace rgw::sal
