#include "query/Query6ClientObject.h"

#include <cassert>
#include <cstring>

void
Query6ClientObject::merge(const char *buffer, size_t bufferSize)
{
    assert(bufferSize == sizeof(Query6Result));
    Query6Result new_result = *reinterpret_cast<Query6Result*>(const_cast<char *>(buffer));
    if (_result.max_local_day < new_result.max_local_day) {
        _result.max_local_day = new_result.max_local_day;
        _result.max_local_day_id = new_result.max_local_day_id;
    }
    if (_result.max_local_week < new_result.max_local_week) {
        _result.max_local_week = new_result.max_local_week;
        _result.max_local_week_id = new_result.max_local_week_id;
    }
    if (_result.max_long_day < new_result.max_long_day) {
        _result.max_long_day = new_result.max_long_day;
        _result.max_long_day_id = new_result.max_long_day_id;
    }
    if (_result.max_long_week < new_result.max_long_week) {
        _result.max_long_week = new_result.max_long_week;
        _result.max_long_week_id = new_result.max_long_week_id;
    }
}

std::vector<char>
Query6ClientObject::getResult()
{
    return std::vector<char>();
}
