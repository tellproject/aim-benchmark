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
#include "Query7ServerObject.h"

#include "util/dimension-tables-unique-values.h"

Query7ServerObject::Query7ServerObject(uint32_t queryNumber,
                                       uint8_t numberOfThreads,
                                       AbstractRTACommunication *communicationObject,
                                       std::vector<char> additionalArguments,
                                       const AIMSchema &aim_schema,
                                       const DimensionSchema &dim_schema,
                                       WindowLength window_length,
                                       uint16_t subscriber_value_type):

    AbstractQueryServerObject(queryNumber, numberOfThreads, communicationObject,
                              std::move(additionalArguments)),
    _value_type_id(subscriber_value_type),
    _window_length(window_length),
    _cost_sum_all_day(aim_schema),
    _cost_sum_all_week(aim_schema),
    _dur_sum_all_day(aim_schema),
    _dur_sum_all_week(aim_schema),
    _subscriber_value_type(aim_schema, dim_schema),
    _subscriber_id(aim_schema, dim_schema)
{}

void
Query7ServerObject::processBucket(uint thread_id, const BucketReference& bucket)
{
    assert(bucket.num_of_records == RECORDS_PER_BUCKET);
    const double* cost_sum;
    const uint32_t* duration_sum;
    if (_window_length == WindowLength::DAY) {
        cost_sum = _cost_sum_all_day.get(bucket);
        duration_sum = _dur_sum_all_day.get(bucket);
    }
    else {
        cost_sum = _cost_sum_all_week.get(bucket);
        duration_sum = _dur_sum_all_week.get(bucket);
    }
    q7_simple(_subscriber_value_type.get(bucket),
              _value_type_id, _subscriber_id.get(bucket),
              cost_sum, duration_sum,
              RECORDS_PER_BUCKET, _entries[thread_id]);
}

void
Query7ServerObject::processLastBucket(uint thread_id, const BucketReference& bucket)
{
    const double* cost_sum;
    const uint32_t* duration_sum;
    if (_window_length == WindowLength::DAY) {
        cost_sum = _cost_sum_all_day.get(bucket);
        duration_sum = _dur_sum_all_day.get(bucket);
    }
    else {
        cost_sum = _cost_sum_all_week.get(bucket);
        duration_sum = _dur_sum_all_week.get(bucket);
    }
    q7_simple(_subscriber_value_type.get(bucket),
              _value_type_id, _subscriber_id.get(bucket),
              cost_sum, duration_sum,
              bucket.num_of_records, _entries[thread_id]);
}

auto
Query7ServerObject::popResult() -> std::pair<std::vector<char>, Status>
{
    Query7Result q7_res;
    for (Query7Result& thread_result: _entries) {
        if (q7_res.avg_money_per_time > thread_result.avg_money_per_time) {
            q7_res.avg_money_per_time = thread_result.avg_money_per_time;
            q7_res.subscriber_id = thread_result.subscriber_id;
        }
    }
    std::vector<char> res(sizeof(Query7Result));
    std::memcpy(res.data(), &q7_res, sizeof(q7_res));
    return std::make_pair(std::move(res), Status::DONE);
}
