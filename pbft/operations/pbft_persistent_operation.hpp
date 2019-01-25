// Copyright (C) 2018 Bluzelle
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License, version 3,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#pragma once

#include <pbft/operations/pbft_operation.hpp>
#include <storage/storage_base.hpp>
#include <proto/pbft.pb.h>

namespace bzn
{
    class pbft_persistent_operation : public pbft_operation
    {
    public:
        pbft_persistent_operation(uint64_t view, uint64_t sequence, const bzn::hash_t& request_hash, std::shared_ptr<bzn::storage_base> storage, size_t );

        void record_pbft_msg(const pbft_msg& msg, const bzn_envelope& encoded_msg) override;

        pbft_operation_stage get_stage() const override;
        void advance_operation_stage(pbft_operation_stage new_stage) override;
        bool is_preprepared() const override;
        bool is_prepared() const override;
        bool is_committed() const override;

        void record_request(const bzn_envelope& encoded_request) override;
        bool has_request() const override;
        bool has_db_request() const override;
        bool has_config_request() const override;

        const bzn_envelope& get_request() const override;
        const pbft_config_msg& get_config_request() const override;
        const database_msg& get_database_msg() const override;

        bzn_envelope get_preprepare() const override;
        std::map<bzn::uuid_t, bzn_envelope> get_prepares() const override;

        static std::string generate_prefix(uint64_t view, uint64_t sequence, const bzn::hash_t& request_hash);
        static std::string generate_key(const std::string& prefix, const std::string& key);
        static std::string key_for_sequence(uint64_t sequence);
        static const std::string& get_uuid();

    private:
        std::string typed_prefix(pbft_msg_type pbft_type) const;
        void load_transient_request() const;

        const size_t peers_size;
        const std::shared_ptr<bzn::storage_base> storage;
        const std::string prefix;

        mutable bool transient_request_available = false;
        mutable bzn_envelope transient_request;
        mutable database_msg transient_database_request;
        mutable pbft_config_msg transient_config_request;
    };

}
