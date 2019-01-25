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

#include <storage/mem_storage.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/map.hpp>
#include <regex>
#include <sstream>

using namespace bzn;


bzn::storage_result
mem_storage::create(const bzn::uuid_t& uuid, const std::string& key, const std::string& value)
{
    std::lock_guard<std::shared_mutex> lock(this->lock); // lock for write access

    if (value.size() > bzn::MAX_VALUE_SIZE)
    {
        return bzn::storage_result::value_too_large;
    }

    if (key.size() > bzn::MAX_KEY_SIZE)
    {
        return bzn::storage_result::key_too_large;
    }

    if (auto search = this->kv_store.find(uuid); search != this->kv_store.end())
    {
        if (search->second.find(key)!= search->second.end() )
        {
            return bzn::storage_result::exists;
        }
    }

    auto& inner_db =  this->kv_store[uuid];

    if (inner_db.find(key) == inner_db.end())
    {
        // todo: test if insert failed?
        inner_db.insert(std::make_pair(key,value));
    }
    else
    {
        return bzn::storage_result::exists;
    }

    return bzn::storage_result::ok;
}


std::optional<bzn::value_t>
mem_storage::read(const bzn::uuid_t& uuid, const std::string& key)
{
    std::shared_lock<std::shared_mutex> lock(this->lock); // lock for read access

    auto search = this->kv_store.find(uuid);

    if (search == this->kv_store.end())
    {
        return std::nullopt;
    }

    // we have the db, let's see if the key exists
    auto& inner_db = search->second;
    auto inner_search = inner_db.find(key);
    if (inner_search == inner_db.end())
    {
        return std::nullopt;
    }
    return inner_search->second;
}


bzn::storage_result
mem_storage::update(const bzn::uuid_t& uuid, const std::string& key, const std::string& value)
{
    std::lock_guard<std::shared_mutex> lock(this->lock); // lock for write access

    if (value.size() > bzn::MAX_VALUE_SIZE)
    {
        return bzn::storage_result::value_too_large;
    }

    auto search = this->kv_store.find(uuid);

    if (search == this->kv_store.end())
    {
        return bzn::storage_result::not_found;
    }


    // we have the db, let's see if the key exists
    auto& inner_db = search->second;
    auto inner_search = inner_db.find(key);

    if (inner_search == inner_db.end())
    {
        return bzn::storage_result::not_found;
    }

    inner_search->second = value;
    return bzn::storage_result::ok;
}


bzn::storage_result
mem_storage::remove(const bzn::uuid_t& uuid, const std::string& key)
{
    std::lock_guard<std::shared_mutex> lock(this->lock); // lock for write access

    auto search = this->kv_store.find(uuid);

    if (search == this->kv_store.end())
    {
        return bzn::storage_result::not_found;
    }

    auto record = search->second.find(key);

    if (record == search->second.end())
    {
        return bzn::storage_result::not_found;
    }

    search->second.erase(record);
    return bzn::storage_result::ok;
}


std::vector<std::string>
mem_storage::get_keys(const bzn::uuid_t& uuid)
{
    std::shared_lock<std::shared_mutex> lock(this->lock); // lock for read access

    auto inner_db = this->kv_store.find(uuid);

    if (inner_db == this->kv_store.end())
    {
        return {};
    }

    std::vector<std::string> keys;
    for (const auto& p : inner_db->second)
    {
        keys.emplace_back(p.first);
    }

    return keys;
}


bool
mem_storage::has(const bzn::uuid_t& uuid, const std::string& key)
{
    const auto v = this->get_keys(uuid);
    return std::find(v.begin(), v.end(), key) != v.end();
}


// todo: optimize! Track size as it grows and not iterate over every value!
std::pair<std::size_t, std::size_t>
mem_storage::get_size(const bzn::uuid_t& uuid)
{
    std::shared_lock<std::shared_mutex> lock(this->lock); // lock for read access

    auto it = this->kv_store.find(uuid);

    if (it == this->kv_store.end())
    {
        // database not found...
        return std::make_pair(0,0);
    }

    std::size_t size{};
    std::size_t keys{};

    for (const auto& record : it->second)
    {
        ++keys;
        size += record.second.size();
    }

    return std::make_pair(keys, size);
}


bzn::storage_result
mem_storage::remove(const bzn::uuid_t& uuid)
{
    std::lock_guard<std::shared_mutex> lock(this->lock); // lock for write access

    if (auto it = this->kv_store.find(uuid); it != this->kv_store.end())
    {
        this->kv_store.erase(it);

        return bzn::storage_result::ok;
    }

    return bzn::storage_result::not_found;
}


bool
mem_storage::create_snapshot()
{
    std::shared_lock<std::shared_mutex> lock(this->lock); // lock for read access

    try
    {
        std::stringstream strm;
        boost::archive::text_oarchive archive(strm);
        archive << this->kv_store;
        this->latest_snapshot = std::make_shared<std::string>(strm.str());

        return true;
    }
    catch (std::exception& ex)
    {
        LOG(error) << "Exception creating snapshot: " << ex.what();
    }

    return false;
}


std::shared_ptr<std::string>
mem_storage::get_snapshot()
{
    std::shared_lock<std::shared_mutex> lock(this->lock); // lock for read access
    return this->latest_snapshot;
}


bool
mem_storage::load_snapshot(const std::string& data)
{
    std::shared_lock<std::shared_mutex> lock(this->lock); // lock for write access

    try
    {
        std::stringstream strm(data);
        boost::archive::text_iarchive archive(strm);
        archive >> this->kv_store;
        this->latest_snapshot = std::make_shared<std::string>(data);

        return true;
    }
    catch (std::exception& ex)
    {
        LOG(error) << "Exception loading snapshot: " << ex.what();
    }

    return false;
}

void
mem_storage::remove_range(const bzn::uuid_t& uuid, const std::string& begin_key, const std::string& end_key)
{
    std::shared_lock<std::shared_mutex> lock(this->lock); // lock for read access

    auto inner_db = this->kv_store.find(uuid);

    if (inner_db != this->kv_store.end())
    {
        auto match = inner_db->second.lower_bound(begin_key);
        auto end = inner_db->second.lower_bound(end_key);
        while (match != end)
        {
            match = inner_db->second.erase(match);
        }
    }
}

std::vector<bzn::key_t>
mem_storage::get_keys_starting_with(const bzn::uuid_t& uuid, const std::string& prefix)
{
    std::shared_lock<std::shared_mutex> lock(this->lock); // lock for read access

    auto inner_db = this->kv_store.find(uuid);

    std::vector<std::string> keys;
    if (inner_db != this->kv_store.end())
    {
        for (auto it = inner_db->second.lower_bound(prefix); it != inner_db->second.end()
            && !it->first.compare(0, prefix.size(), prefix); it++)
        {
            keys.emplace_back(it->first);
        }
    }

    return keys;
}

// find matching records in storage based on a limited regular expression.
// literals at the beginning of pattern can be used to optimize where the search starts and (if no end is specified),
// where it ends. If end is specified then a range between the literals in start and end is searched for matches.
// If there are no literals at the start of the pattern, searching begins at the start of records matching uuid.
// If end is the empty string, searching continues to the end of records matching uuid.
std::vector<std::pair<bzn::key_t, bzn::value_t>>
mem_storage::get_matching(const bzn::uuid_t& uuid, const std::string& pattern, std::optional<std::string> end)
{
    std::shared_lock<std::shared_mutex> lock(this->lock); // lock for read access

    auto start_key = this->make_start_prefix(pattern);
    auto end_key = end ? *end : this->make_end_prefix(start_key);
    auto fixed_size = start_key.size();
    const std::regex exp(fixed_size < pattern.size() ? pattern.substr(fixed_size) : ".*");

    std::vector<std::pair<bzn::key_t, bzn::value_t>> matches;

    auto inner_db = this->kv_store.find(uuid);
    if (inner_db != this->kv_store.end())
    {
        auto end_it = end_key.empty() ? inner_db->second.end() : inner_db->second.lower_bound(end_key);
        for (auto it = inner_db->second.lower_bound(start_key); it != inner_db->second.end() && it != end_it; it++)
        {
            if (it->first.compare(0, fixed_size, start_key) >= 0 &&
                (end_key.empty() || it->first.compare(0, fixed_size, end_key) <= 0) &&
                std::regex_search(it->first.substr(fixed_size), exp, std::regex_constants::match_continuous))
            {
                matches.push_back(std::make_pair(it->first, it->second));
            }
        }
    }

    return matches;
}

std::string
mem_storage::make_start_prefix(const std::string& pattern)
{
    return pattern.substr(0, pattern.find_first_of("[\\^$.|?*+()"));
}

std::string
mem_storage::make_end_prefix(const std::string& start_prefix)
{
    auto end_prefix = start_prefix;
    if (!start_prefix.empty())
    {
        assert(end_prefix[end_prefix.size() - 1] < 0x7f);
        end_prefix[end_prefix.size() - 1]++;
    }
    return end_prefix;
}