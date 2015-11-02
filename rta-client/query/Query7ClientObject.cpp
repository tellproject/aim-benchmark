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
