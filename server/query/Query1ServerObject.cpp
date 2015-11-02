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
#include "Query1ServerObject.h"

#include "column-map/ColumnMap.h"

Query1ServerObject::Query1ServerObject(uint32_t query_id, uint8_t threads_num,
                                       AbstractRTACommunication* communication,
                                       std::vector<char> additional_args,
                                       const AIMSchema &aim_schema,
                                       uint32_t alpha):

    AbstractQueryServerObject(query_id, threads_num, communication,
                              std::move(additional_args)),
    _alpha(alpha),
    _calls_sum_local_week(aim_schema),
    _dur_sum_all_week(aim_schema)
{
    for (auto& val : _sums) {
        val = 0;
    }
    for (auto& val : _counts) {
        val = 0;
    }
}

void
Query1ServerObject::processBucket(uint thread_id, const BucketReference &bucket)
{
    assert(bucket.num_of_records == RECORDS_PER_BUCKET);
    auto avg = q1_simple<RECORDS_PER_BUCKET>(_calls_sum_local_week.get(bucket), _alpha,
                                             _dur_sum_all_week.get(bucket));
    _sums[thread_id] += avg.sum;
    _counts[thread_id] += avg.count;
}

void
Query1ServerObject::processLastBucket(uint thread_id, const BucketReference &bucket)
{
    auto avg = q1_simple(_calls_sum_local_week.get(bucket), _alpha,
                         _dur_sum_all_week.get(bucket),
                         bucket.num_of_records);

    _sums[thread_id] += avg.sum;
    _counts[thread_id] += avg.count;
}

auto
Query1ServerObject::popResult() -> std::pair<std::vector<char>, Status>
{
    uint64_t sum = std::accumulate(_sums.begin(), _sums.end(), uint64_t(0));
    uint32_t count = std::accumulate(_counts.begin(), _counts.end(), uint32_t(0));
    std::vector<char> resultBuf(sizeof(sum) + sizeof(count));
    *(reinterpret_cast<decltype(sum) *>(resultBuf.data())) = sum;
    *(reinterpret_cast<decltype(count) *>(&resultBuf[sizeof(sum)])) = count;
    return std::make_pair(std::move(resultBuf), Status::DONE);
}
