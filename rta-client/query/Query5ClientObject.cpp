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
