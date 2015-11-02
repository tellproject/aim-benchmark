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

#include <array>

#include "query/AbstractQueryServerObject.h"

/*
 * Q2: SELECT max(most_expensive_call_this_week)
 * FROM WT
 * WHERE number_of_calls_this_week > alpha;
 */
class Query2ServerObject : public AbstractQueryServerObject
{
public:
    Query2ServerObject(uint32_t query_id, uint8_t threads_num,
                       AbstractRTACommunication* communication,
                       std::vector<char> additional_args,
                       const AIMSchema &aim_schema, uint32_t alpha);

     ~Query2ServerObject() = default;

public:
    void processBucket(uint thread_id, const BucketReference&) override;
    void processLastBucket(uint thread_id, const BucketReference&) override;

    /**
     * @brief popResult
     * @return double MAX
     */
    std::pair<std::vector<char>, Status> popResult() override;

private:
    AimColumnFinder<Metric::COST, AggrFun::MAX, FilterType::NO, WindowLength::WEEK> _cost_max_all_week;
    AimColumnFinder<Metric::CALL, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _calls_sum_all_week;
    uint32_t _alpha;

    std::array<double, RTA_THREAD_NUM> _maxs;
};
