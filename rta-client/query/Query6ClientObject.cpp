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
