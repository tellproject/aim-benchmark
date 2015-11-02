#include "query/Query7ClientObject.h"

#include <cassert>
#include <cstring>

void
Query7ClientObject::merge(const char *buffer, size_t bufferSize)
{
    assert(bufferSize == sizeof(Query7Result));
    Query7Result new_result = *reinterpret_cast<Query7Result*>(const_cast<char *>(buffer));
    if (_result.avg_money_per_time > new_result.avg_money_per_time) {
        _result.avg_money_per_time = new_result.avg_money_per_time;
        _result.subscriber_id = new_result.subscriber_id;
    }
}

std::vector<char>
Query7ClientObject::getResult()
{
    return std::vector<char>();
}
