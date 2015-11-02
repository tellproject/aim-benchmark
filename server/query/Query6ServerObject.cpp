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
#include "Query6ServerObject.h"

#include "util/dimension-tables-unique-values.h"

Query6ServerObject::Query6ServerObject(uint32_t query_id,
                                       uint8_t threads_num,
                                       AbstractRTACommunication *communication,
                                       std::vector<char> additional_args,
                                       const AIMSchema &aim_schema,
                                       const DimensionSchema &dim_schema,
                                       uint16_t country_id):

    AbstractQueryServerObject(query_id, threads_num, communication,
                              std::move(additional_args)),
    _country_id(country_id),
    _dur_max_local_day(aim_schema),
    _dur_max_local_week(aim_schema),
    _dur_max_long_day(aim_schema),
    _dur_max_long_week(aim_schema),
    _region_country(aim_schema, dim_schema),
    _subscriber_id(aim_schema, dim_schema)
{}

void
Query6ServerObject::processBucket(uint thread_id, const BucketReference& bucket)
{
    assert(bucket.num_of_records == RECORDS_PER_BUCKET);
    q6_simple(_region_country.get(bucket),
              _country_id,
              _subscriber_id.get(bucket),
              _dur_max_local_week.get(bucket),
              _dur_max_local_day.get(bucket),
              _dur_max_long_week.get(bucket),
              _dur_max_long_day.get(bucket),
              RECORDS_PER_BUCKET, _entries[thread_id]);
}

void
Query6ServerObject::processLastBucket(uint thread_id, const BucketReference& bucket)
{
    q6_simple(_region_country.get(bucket),
              _country_id,
              _subscriber_id.get(bucket),
              _dur_max_local_week.get(bucket),
              _dur_max_local_day.get(bucket),
              _dur_max_long_week.get(bucket),
              _dur_max_long_day.get(bucket),
              bucket.num_of_records, _entries[thread_id]);
}

auto
Query6ServerObject::popResult() -> std::pair<std::vector<char>, Status>
{
    Query6Result q6_res;
    for (Query6Result& thread_result: _entries) {
        if (q6_res.max_local_day < thread_result.max_local_day) {
            q6_res.max_local_day = thread_result.max_local_day;
            q6_res.max_local_day_id = thread_result.max_local_day_id;
        }
        if (q6_res.max_local_week < thread_result.max_local_week) {
            q6_res.max_local_week = thread_result.max_local_week;
            q6_res.max_local_week_id = thread_result.max_local_week_id;
        }
        if (q6_res.max_long_day < thread_result.max_long_day) {
            q6_res.max_long_day = thread_result.max_long_day;
            q6_res.max_long_day_id = thread_result.max_long_day_id;
        }
        if (q6_res.max_long_week < thread_result.max_long_week) {
            q6_res.max_long_week = thread_result.max_long_week;
            q6_res.max_long_week_id = thread_result.max_long_week_id;
        }
    }
    std::vector<char> res(sizeof(Query6Result));
    std::memcpy(res.data(), &q6_res, sizeof(q6_res));
    return std::make_pair(std::move(res), Status::DONE);
}
