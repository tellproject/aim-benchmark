/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include "query/Query5ClientObject.h"

#include <cassert>
#include <cstring>

#include "util/serialization.h"

Query5ClientObject::Query5ClientObject()
{}

/*
 * The buffer should have the following format:
 *
 * city_name_length|city_name|avg_sum|avg_cnt|sum
 *
 * that way we can parse it correctly and extract the information we need.
 */
void
Query5ClientObject::merge(const char *buffer, size_t bufferSize)
{
    auto tmp = buffer;
    while (tmp < buffer + bufferSize) {
        std::string region_name;
        RegionEntry new_region_entry;
        deserialize(tmp, region_name);
        deserialize(tmp, new_region_entry);
        auto it = _entries.find(region_name);
        if (it == _entries.end()) {     //first result for this city
            _entries.emplace(region_name, new_region_entry);
        }
        else {                          //Entry already exists
            RegionEntry &region_entry = it->second;
            region_entry.local_sum += new_region_entry.local_sum;
            region_entry.long_sum += new_region_entry.long_sum;
        }
    }
    assert(tmp == buffer + bufferSize);
}

std::vector<char>
Query5ClientObject::getResult()
{
    return std::vector<char>();
}
