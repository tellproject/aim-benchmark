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
#include <cstdio>
#include <cstring>
#include <mutex>

#include "common//system-constants.h"
#include "server/query/AbstractQueryServerObject.h"

/*
 * Q3: SELECT sum(total_cost_this_week)/sum(total_duration_this_week) as cost_ratio
 * FROM WT
 * GROUP BY number_of_calls_this_week
 */
class Query3ServerObject : public AbstractQueryServerObject
{
public:
    Query3ServerObject(uint32_t query_id, uint8_t threads_num,
                       AbstractRTACommunication* communication,
                       std::vector<char> additional_args,
                       const AIMSchema &aim_schema);

    ~Query3ServerObject() = default;

public:
    void processBucket(uint thread_id, const BucketReference&) override;
    void processLastBucket(uint thread_id, const BucketReference&) override;
    std::pair<std::vector<char>, Status> popResult() override;

private:

    AimColumnFinder<Metric::CALL, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _calls_sum_all_week;
    AimColumnFinder<Metric::COST, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _cost_sum_all_week;
    AimColumnFinder<Metric::DUR, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _dur_sum_all_week;
    std::array<Q3Result, RTA_THREAD_NUM> _active_results;
    Status _status = Status::HAS_NEXT;
};
