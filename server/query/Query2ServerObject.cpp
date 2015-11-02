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
#include "Query2ServerObject.h"

Query2ServerObject::Query2ServerObject(uint32_t query_id, uint8_t threads_num,
                                       AbstractRTACommunication* communication,
                                       std::vector<char> additional_args,
                                       const AIMSchema &aim_schema, uint32_t alpha):

    AbstractQueryServerObject(query_id, threads_num, communication,
                              std::move(additional_args)),
    _cost_max_all_week(aim_schema),
    _calls_sum_all_week(aim_schema),
    _alpha(alpha)
{
    for (auto& val: _maxs) {
        val = std::numeric_limits<decltype(_maxs)::value_type>::min();
    }
}

void
Query2ServerObject::processBucket(uint thread_id, const BucketReference &bucket)
{
    assert(bucket.num_of_records == RECORDS_PER_BUCKET);
    auto result = q2_simd<RECORDS_PER_BUCKET>(_cost_max_all_week.get(bucket),
                                              _calls_sum_all_week.get(bucket), _alpha);
    _maxs[thread_id] = std::max(_maxs[thread_id], result);
}

void
Query2ServerObject::processLastBucket(uint thread_id, const BucketReference &bucket)
{
    auto result = q2_simple(_cost_max_all_week.get(bucket),
                            _calls_sum_all_week.get(bucket),
                            _alpha, bucket.num_of_records);
    _maxs[thread_id] = std::max(_maxs[thread_id], result);
}

auto
Query2ServerObject::popResult() -> std::pair<std::vector<char>, Status>
{
    double result = *std::max_element(_maxs.begin(), _maxs.end());
    std::vector<char> resultBuf(sizeof(result));
    *(reinterpret_cast<decltype(result)*>(resultBuf.data())) = result;
    return std::make_pair(std::move(resultBuf), Status::DONE);
}
