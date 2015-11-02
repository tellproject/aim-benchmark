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

#include <string>
#include <unordered_map>

#include "query/AbstractQueryClientObject.h"

/*
 * This class implements the logic for the Call-intensive Cities query
 * on the client side. Query 6.2.1:
 *
 * SELECT city, avg(number of local calls this week),
 *        sum(total duration of local calls this week)
 * FROM VWT, RegionInfo
 * WHERE number of local calls this week > numberThreshold AND
 * total duration of local calls this week > durationThreshold AND
 * VWT.zip = RegionInfo.zip
 * GROUP BY city
 */
class Query4ClientObject : public AbstractQueryClientObject
{
public:
    struct CityEntry
    {
        uint64_t avg_sum = 0;
        uint32_t avg_cnt = 0;
        uint64_t sum = 0;
    };

public:
    Query4ClientObject();
    ~Query4ClientObject() = default;

public:
    void merge(const char *buffer, size_t bufferSize) override;
    std::vector<char> getResult() override;

private:
    std::unordered_map<std::string, CityEntry> _entries;
};
