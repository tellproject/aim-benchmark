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

#include "query/AbstractQueryServerObject.h"

#include <array>

#include "rta/dim_column_finder.h"

/*
 * This class implements the logic for the Highest Revenue query
 * on the server side. Query 6.2.2:
 *
 * SELECT region, sum(total cost of local calls this week) as local,
 *        sum(total cost of long distance calls) as long_distance
 * FROM VWT, SubscriptionType t, SubscriberCategory c, RegionInfo r
 * WHERE t.type = subscriptionType AND c.type = subscriberCategory AND
 *       VWT.subscription-type = t.id AND VWT.category = c.id AND
 *       VWT.zip = r.zip
 * GROUP BY region
 */
class Query5ServerObject: public AbstractQueryServerObject
{
public:
    struct RegionEntry
    {
        double local_sum = 0;
        double long_sum = 0;
    };

public:
    Query5ServerObject(uint32_t query_id, uint8_t threads_num,
                       AbstractRTACommunication* communication,
                       std::vector<char> additional_args,
                       const AIMSchema &aim_schema,
                       const DimensionSchema& dim_schema,
                       uint16_t type, uint16_t category);

    ~Query5ServerObject() = default;

public:
    void processBucket(uint thread_id, const BucketReference&) override;
    void processLastBucket(uint thread_id, const BucketReference&) override;
    std::pair<std::vector<char>, Status> popResult() override;

private:
    const uint16_t _type;
    const uint16_t _category;
    AimColumnFinder<Metric::COST, AggrFun::SUM, FilterType::LOCAL, WindowLength::WEEK> _cost_sum_local_week;
    AimColumnFinder<Metric::COST, AggrFun::SUM, FilterType::NONLOCAL, WindowLength::WEEK> _cost_sum_long_week;
    DimColumnFinder<DimensionAttribute::REGION_REGION> _region_region;
    DimColumnFinder<DimensionAttribute::SUBSCRIPTION_TYPE> _subscription_type;
    DimColumnFinder<DimensionAttribute::SUBSCRIBER_CATEGORY_TYPE> _subscriber_category;

    std::array<std::vector<RegionEntry>, RTA_THREAD_NUM> _entries;
};
