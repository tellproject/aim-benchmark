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
 * This class implements the logic for the Highest Revenue query
 * on the client side. Query 6.2.2:
 *
 * SELECT region, sum(total cost of local calls this week) as local,
 *        sum(total cost of long distance calls) as long_distance
 * FROM VWT, SubscriptionType t, SubscriberCategory c, RegionInfo r
 * WHERE t.type = subscriptionType AND c.type = subscriberCategory AND
 *       VWT.subscription-type = t.id AND VWT.category = c.id AND
 *       VWT.zip = r.zip
 * GROUP BY region
 */
class Query5ClientObject : public AbstractQueryClientObject
{
public:
    struct RegionEntry
    {
        double local_sum = 0;
        double long_sum = 0;
    };
public:
    Query5ClientObject();
    ~Query5ClientObject() = default;

public:
    void merge(const char *buffer, size_t bufferSize) override;
    std::vector<char> getResult() override;

private:
    std::unordered_map<std::string, RegionEntry> _entries;
};

