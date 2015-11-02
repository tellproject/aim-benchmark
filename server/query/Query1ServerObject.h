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
 * Q1: SELECT avg(total_duration_this_week)
 * FROM WT
 * WHERE number_of_local_calls_this_week > alpha;
 */
class Query1ServerObject: public AbstractQueryServerObject
{
public:
    Query1ServerObject(uint32_t query_id, uint8_t threads_num,
                       AbstractRTACommunication *communication,
                       std::vector<char> additional_args,
                       const AIMSchema& aim_schema, uint32_t alpha);

    ~Query1ServerObject() = default;

public:
    void processBucket(uint thread_id, const BucketReference&) override;
    void processLastBucket(uint thread_id, const BucketReference&) override;

    /**
     * @brief popResult
     * @return  uint64_t SUM | uint32_t COUNT
     */
    std::pair<std::vector<char>, Status> popResult() override;

private:
    const uint32_t _alpha;
    AimColumnFinder<Metric::CALL, AggrFun::SUM, FilterType::LOCAL, WindowLength::WEEK> _calls_sum_local_week;
    AimColumnFinder<Metric::DUR, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _dur_sum_all_week;
    std::array<uint64_t, RTA_THREAD_NUM> _sums;
    std::array<uint32_t, RTA_THREAD_NUM> _counts;
};
