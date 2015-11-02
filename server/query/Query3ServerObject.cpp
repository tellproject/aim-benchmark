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
#include "Query3ServerObject.h"

#include <iostream>
#include <thread>

Query3ServerObject::Query3ServerObject(uint32_t query_id, uint8_t threads_num,
                                       AbstractRTACommunication* communication,
                                       std::vector<char> additional_args,
                                       const AIMSchema &aim_schema):

    AbstractQueryServerObject(query_id, threads_num, communication,
                              std::move(additional_args)),
    _calls_sum_all_week(aim_schema),
    _cost_sum_all_week(aim_schema),
    _dur_sum_all_week(aim_schema)
{}

void
Query3ServerObject::processBucket(uint thread_id, const BucketReference &bucket)
{
    assert(bucket.num_of_records == RECORDS_PER_BUCKET);
    q3_simple<RECORDS_PER_BUCKET>(_cost_sum_all_week.get(bucket),
                                  _dur_sum_all_week.get(bucket),
                                  _calls_sum_all_week.get(bucket),
                                  _active_results[thread_id]);
}

void
Query3ServerObject::processLastBucket(uint thread_id, const BucketReference &bucket)
{
    q3_simple(_cost_sum_all_week.get(bucket),
              _dur_sum_all_week.get(bucket),
              _calls_sum_all_week.get(bucket),
              bucket.num_of_records, _active_results[thread_id]);
}

auto
Query3ServerObject::popResult() -> std::pair<std::vector<char>, Status>
{
    Q3Result result;
    for (Q3Result& partial_result : _active_results) {
        for (uint i=0; i<Q3Result::SMALL_THRESHOLD; ++i) {
            result.small_values[i] += partial_result.small_values[i];
        }
        for (auto iter = partial_result.outliers.begin(); iter != partial_result.outliers.end(); ++iter) {
            result.outliers[iter->first] += iter->second;
        }
    }
    auto number_of_result_tuples = result.outliers.size();
    for (uint i=0; i<Q3Result::SMALL_THRESHOLD; ++i) {
        if (result.small_values[i].cost_sum_all_week) {
            ++number_of_result_tuples;
        }
    }
    size_t result_size = sizeof(number_of_result_tuples) + number_of_result_tuples * sizeof(Q3Tuple);
    std::vector<char> resultBuf(result_size);
    char* tmp = resultBuf.data();
    memcpy(tmp, &number_of_result_tuples, sizeof(number_of_result_tuples));
    tmp += sizeof(number_of_result_tuples);
    for (uint32_t i=0; i<Q3Result::SMALL_THRESHOLD; ++i) {
        if (result.small_values[i].cost_sum_all_week) {
            memcpy(tmp, &result.small_values[i], sizeof(Q3Tuple));
            tmp += sizeof(Q3Tuple);
        }
    }

    for (auto iter = result.outliers.begin(); iter != result.outliers.end(); ++iter) {
        memcpy(tmp, &iter->second, sizeof(Q3Tuple));
        tmp += sizeof(Q3Tuple);
    }
    assert(tmp == resultBuf.data() + resultBuf.size());
    return std::make_pair(std::move(resultBuf), Status::DONE);
}
