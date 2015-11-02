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
#pragma once

#include "query/AbstractQueryClientObject.h"

class Query6ClientObject : public AbstractQueryClientObject
{
public:
    struct Query6Result
    {
        uint64_t max_local_week_id = 0;
        uint32_t max_local_week = 0;
        uint64_t max_local_day_id = 0;
        uint32_t max_local_day = 0;
        uint64_t max_long_week_id = 0;
        uint32_t max_long_week = 0;
        uint64_t max_long_day_id = 0;
        uint32_t max_long_day = 0;
    };
public:
    ~Query6ClientObject() = default;

public:
    void merge(const char *buffer, size_t bufferSize) override;
    std::vector<char> getResult() override;

private:
    Query6Result _result;
};

