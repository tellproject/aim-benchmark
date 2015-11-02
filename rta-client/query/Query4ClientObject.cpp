#include "query/Query4ClientObject.h"

#include <cassert>
#include <cstring>

Query4ClientObject::Query4ClientObject()
{}

/*
 * The buffer should have the following format:
 *
 * city_name_length|city_name|avg_sum|avg_cnt|sum
 *
 * that way we can parse it correctly and extract the information we need.
 */
void
Query4ClientObject::merge(const char *buffer, size_t bufferSize)
{
    auto tmp = buffer;
    while (tmp < buffer + bufferSize) {
        std::string city_name;
        CityEntry city_entry;
        deserialize(tmp, city_name);
        deserialize(tmp, city_entry);
        auto it = _entries.find(city_name);
        if (it == _entries.end()) {     //first result for this city
            _entries.emplace(city_name, city_entry);
        }
        else {                          //Entry already exists
            CityEntry &city_entry = it->second;
            city_entry.avg_sum += city_entry.avg_sum;
            city_entry.avg_cnt += city_entry.avg_cnt;
            city_entry.sum += city_entry.sum;
        }

/*
    size_t city_name_len = *reinterpret_cast<const size_t*>(buffer);
    char tmp_city_name[city_name_len];
    memset(tmp_city_name, '0', city_name_len);
    memcpy(tmp_city_name, buffer+sizeof(size_t), city_name_len);
    std::string city_name(tmp_city_name);

    uint64_t avg_sum = *reinterpret_cast<const uint64_t*>(&buffer[sizeof(size_t) +
                                                            city_name_len]);

    uint32_t avg_cnt = *reinterpret_cast<const uint32_t*>(&buffer[sizeof(size_t) +
                                                            city_name_len] +
                                                            sizeof(uint64_t));

    uint64_t sum = *reinterpret_cast<const uint64_t*>(&buffer[sizeof(size_t) +
                                                        city_name_len] +
                                                        sizeof(uint64_t) +
                                                        sizeof(uint32_t));

    auto it = _entries.find(city_name);
    if (it == _entries.end()) {     //first result for this city
        _entries.emplace(city_name, CityEntry{avg_sum, avg_cnt, sum, city_name_len});
        _cities_names_len += city_name_len;
    }
    else {                          //Entry already exists
        CityEntry &city_entry = it->second;
        city_entry.avg_sum += avg_sum;
        city_entry.avg_cnt += avg_cnt;
        city_entry.sum += sum;
*/
    }
    assert(tmp == buffer + bufferSize);
}

std::vector<char>
Query4ClientObject::getResult()
{
    return std::vector<char>();
}
