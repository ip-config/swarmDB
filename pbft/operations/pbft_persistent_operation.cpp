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

#include <pbft/operations/pbft_persistent_operation.hpp>
#include <boost/format.hpp>
#include <include/bluzelle.hpp>
#include <pbft/pbft.hpp>

using namespace bzn;

namespace {
    const std::string STAGE_KEY = "stage";
    const std::string REQUEST_KEY = "request";
    const std::string OPERATIONS_UUID = "pbft_operations_data";
}

std::string
pbft_persistent_operation::generate_prefix(uint64_t view, uint64_t sequence, const bzn::hash_t& request_hash)
{
    // Integers formatted to 20 digits, which is the maximum length of a 64 bit uint- they need to have constant length
    // to be sorted correctly for prefix searches and the like
    return (boost::format("%020u_%s_%020u") % sequence % request_hash % view).str();
}

std::string
pbft_persistent_operation::generate_key(const std::string& prefix, const std::string& key)
{
    // Integers formatted to 20 digits, which is the maximum length of a 64 bit uint- they need to have constant length
    // to be sorted correctly for prefix searches and the like
    return prefix + "_" + key;
}

pbft_persistent_operation::pbft_persistent_operation(uint64_t view, uint64_t sequence, const bzn::hash_t& request_hash, std::shared_ptr<bzn::storage_base> storage, size_t peers_size)
        : pbft_operation(view, sequence, request_hash)
        , peers_size(peers_size)
        , storage(std::move(storage))
        , prefix(pbft_persistent_operation::generate_prefix(view, sequence, request_hash))
{
    const auto response = this->storage->create(OPERATIONS_UUID, generate_key(prefix, STAGE_KEY)
        , std::to_string(static_cast<unsigned int>(pbft_operation_stage::prepare)));
    switch (response)
    {
        case storage_result::ok:
            LOG(info) << "created persistent operation with prefix " <<this->prefix << "; this is our first record of it";
            break;
        case storage_result::exists:
            LOG(info) << "created persistent operation with prefix " <<this->prefix << "; using existing records";
            break;
        default:
            throw std::runtime_error("failed to write stage of new persistent operation " + storage_result_msg.at(response));
    }
}

void
pbft_persistent_operation::record_pbft_msg(const pbft_msg& msg, const bzn_envelope& encoded_msg)
{
    if(msg.type() != pbft_msg_type::PBFT_MSG_PREPREPARE
       && msg.type() != pbft_msg_type::PBFT_MSG_PREPARE
       && msg.type() != pbft_msg_type::PBFT_MSG_COMMIT)
    {
        LOG(error) << "tried to record a pbft message with inappropriate type: " << pbft_msg_type_Name(msg.type());
        return;
    }

    const auto response = this->storage->create(OPERATIONS_UUID
        , generate_key(this->typed_prefix(msg.type()), encoded_msg.sender()), encoded_msg.SerializeAsString());

    switch (response)
    {
        case storage_result::ok:
            LOG(debug) << "saved " << pbft_msg_type_Name(msg.type()) << " from " << encoded_msg.sender() << " for operation " << this->prefix;
            break;
        case storage_result::exists:
            LOG(debug) << "ignored duplicate " << pbft_msg_type_Name(msg.type()) << " from " << encoded_msg.sender() << " for operation " << this->prefix;
            break;
        default:
            throw std::runtime_error("failed to write pbft_msg " + storage_result_msg.at(response));
    }
}

pbft_operation_stage
pbft_persistent_operation::get_stage() const
{
    const auto response = this->storage->read(OPERATIONS_UUID, generate_key(this->prefix, STAGE_KEY));
    if (!response)
    {
        throw std::runtime_error("failed to read stage of pbft_operation " + this->prefix + " from storage");
    }
    return static_cast<pbft_operation_stage>(std::stoi(*response));
}

void
pbft_persistent_operation::advance_operation_stage(pbft_operation_stage new_stage)
{
    switch (new_stage)
    {
        case pbft_operation_stage::prepare :
            throw std::runtime_error("cannot advance to initial stage");
        case pbft_operation_stage::commit :
            if (!this->is_preprepared() || this->get_stage() != pbft_operation_stage::prepare)
            {
                throw std::runtime_error("illegal move to commit phase");
            }
            break;
        case pbft_operation_stage::execute :
            if (!this->is_committed() || this->get_stage() != pbft_operation_stage::commit)
            {
                throw std::runtime_error("illegal move to execute phase");
            }
            break;
        default:
            throw std::runtime_error("unknown pbft_operation_stage: " + std::to_string(static_cast<int>(new_stage)));
    }

    const auto response = this->storage->update(OPERATIONS_UUID, generate_key(this->prefix, STAGE_KEY)
        , std::to_string(static_cast<int>(new_stage)));
    if (response != storage_result::ok)
    {
        throw std::runtime_error("failed to write operation stage update: " + storage_result_msg.at(response));
    }
}

bool
pbft_persistent_operation::is_preprepared() const
{
    return this->storage->get_keys_starting_with(OPERATIONS_UUID
        , this->typed_prefix(pbft_msg_type::PBFT_MSG_PREPREPARE)).size() > 0;
}

bool
pbft_persistent_operation::is_prepared() const
{
    return this->storage->get_keys_starting_with(OPERATIONS_UUID
        , this->typed_prefix(pbft_msg_type::PBFT_MSG_PREPARE)).size() >= pbft::honest_majority_size(this->peers_size)
        && this->is_preprepared() && this->has_request();
}

bool
pbft_persistent_operation::is_committed() const
{
    return this->storage->get_keys_starting_with(OPERATIONS_UUID
        , this->typed_prefix(pbft_msg_type::PBFT_MSG_COMMIT)).size() >= pbft::honest_majority_size(this->peers_size)
           && this->is_prepared();
}

void
pbft_persistent_operation::record_request(const bzn_envelope& encoded_request)
{
    if (this->transient_request_available)
    {
        LOG(debug) << "ignoring record of request for operation " << this->prefix << " because we already have one";
        return;
    }

    const auto response = this->storage->create(OPERATIONS_UUID, generate_key(this->prefix, REQUEST_KEY)
        , encoded_request.SerializeAsString());
    switch (response)
    {
        case storage_result::ok:
            LOG(debug) << "recorded request for operation " << this->prefix;
            break;
        case storage_result::exists:
            LOG(debug) << "ignoring record of request for operation " << this->prefix << " because we already have one";
            break;
        default:
            throw std::runtime_error("failed to write request for operation " + this->prefix);
    }

    // this will allow future calls to record_request to short circuit
    this->load_transient_request();
}

bool
pbft_persistent_operation::has_request() const
{
    this->load_transient_request();
    return this->transient_request_available;
}

void
pbft_persistent_operation::load_transient_request() const
{
    if (this->transient_request_available)
    {
        return;
    }

    const auto response = this->storage->read(OPERATIONS_UUID, generate_key(this->prefix, REQUEST_KEY));
    if (!response.has_value())
    {
        return;
    }

    this->transient_request.ParseFromString(*response);
    this->transient_request_available = true;

    if (this->transient_request.payload_case() == bzn_envelope::kDatabaseMsg)
    {
        this->transient_database_request.ParseFromString(this->transient_request.database_msg());
    }
    else if (this->transient_request.payload_case() == bzn_envelope::kPbftInternalRequest)
    {
        this->transient_config_request.ParseFromString(this->transient_request.pbft_internal_request());
    }
}

bool
pbft_persistent_operation::has_db_request() const
{
    return this->has_request() && this->get_request().payload_case() == bzn_envelope::kDatabaseMsg;
}

bool
pbft_persistent_operation::has_config_request() const
{
    return this->has_request() && this->get_request().payload_case() == bzn_envelope::kPbftInternalRequest;
}

const bzn_envelope&
pbft_persistent_operation::get_request() const
{
    if (!this->has_request())
    {
        throw std::runtime_error("tried to get request of operation " + this->prefix + "; we have no such request");
    }

    return this->transient_request;
}

const pbft_config_msg&
pbft_persistent_operation::get_config_request() const
{
    if (!this->has_config_request())
    {
        throw std::runtime_error("tried to get config request of operation " + this->prefix + "; we have no such request");
    }

    return this->transient_config_request;

}

const database_msg&
pbft_persistent_operation::get_database_msg() const
{
    if (!this->has_db_request())
    {
        throw std::runtime_error("tried to get database request of operation " + this->prefix + "; we have no such request");
    }

    return this->transient_database_request;
}

std::string
pbft_persistent_operation::typed_prefix(pbft_msg_type pbft_type) const
{
    return this->prefix + "_" + std::to_string(pbft_type);
}

bzn_envelope
pbft_persistent_operation::get_preprepare() const
{
    auto keys = this->storage->get_keys_starting_with(OPERATIONS_UUID, this->typed_prefix(pbft_msg_type::PBFT_MSG_PREPREPARE));
    if (keys.size() == 0)
    {
        throw std::runtime_error("tried to fetch a preprepare that we don't have for operation " + this->prefix);
    }

    bzn_envelope env;
    if (!env.ParseFromString(this->storage->read(OPERATIONS_UUID, keys.at(0)).value_or("")))
    {
        throw std::runtime_error("failed to parse or fetch preprepare that we supposedly have? " + this->prefix);
    }

    return env;
}

std::map<bzn::uuid_t, bzn_envelope>
pbft_persistent_operation::get_prepares() const
{
    auto keys = this->storage->get_keys_starting_with(OPERATIONS_UUID, this->typed_prefix(pbft_msg_type::PBFT_MSG_PREPARE));
    std::map<uuid_t, bzn_envelope> result;

    for (const auto& key : keys)
    {
        if (!result[key].ParseFromString(this->storage->read(OPERATIONS_UUID, key).value_or("")))
        {
            throw std::runtime_error("failed to parse or fetch prepare that we supposedly have? " + this->prefix);
        }
    }

    return result;
}
