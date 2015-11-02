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
#include "Query5ServerObject.h"

#include "util/dimension-tables-unique-values.h"

Query5ServerObject::Query5ServerObject(uint32_t query_id, uint8_t threads_num,
                                       AbstractRTACommunication* communication,
                                       std::vector<char> additional_args,
                                       const AIMSchema &aim_schema,
                                       const DimensionSchema& dim_schema,
                                       uint16_t type, uint16_t category):

    AbstractQueryServerObject(query_id, threads_num, communication,
                              std::move(additional_args)),
    _type(type),
    _category(category),
    _cost_sum_local_week(aim_schema),
    _cost_sum_long_week(aim_schema),
    _region_region(aim_schema, dim_schema),
    _subscription_type(aim_schema, dim_schema),
    _subscriber_category(aim_schema, dim_schema)
{
    for (auto& entry : _entries) {
        entry.resize(region_unique_region.size());
    }
}

void
Query5ServerObject::processBucket(uint thread_id, const BucketReference& bucket)
{
    assert(bucket.num_of_records == RECORDS_PER_BUCKET);
    q5_simple<RECORDS_PER_BUCKET>(_region_region.get(bucket),
                                  _cost_sum_local_week.get(bucket),
                                  _cost_sum_long_week.get(bucket),
                                  _subscription_type.get(bucket),
                                  _subscriber_category.get(bucket),
                                  _type, _category, _entries[thread_id]);
}

void
Query5ServerObject::processLastBucket(uint thread_id, const BucketReference& bucket)
{
    q5_simple(_region_region.get(bucket),
              _cost_sum_local_week.get(bucket),
              _cost_sum_long_week.get(bucket),
              _subscription_type.get(bucket),
              _subscriber_category.get(bucket),
              _type, _category, bucket.num_of_records,
              _entries[thread_id]);
}

auto
Query5ServerObject::popResult() -> std::pair<std::vector<char>, Status>
{
    std::vector<RegionEntry> regions(region_unique_region.size());
    for (auto& thread_entries : _entries) {
        assert(thread_entries.size() == regions.size());
        for (size_t i=0; i<thread_entries.size(); ++i) {
            regions[i].local_sum += thread_entries[i].local_sum;
            regions[i].long_sum += thread_entries[i].long_sum;
        }

    }
    size_t res_size = region_unique_region.size() * (2 * sizeof(double) + sizeof(size_t));
    for (std::string &region: region_unique_region) {
        res_size += region.size();
    }
    std::vector<char> res(res_size);
    char *tmp = res.data();
    for (size_t i=0; i<regions.size(); ++i) {
        RegionEntry &entry = regions[i];
        serialize(region_unique_region[i], tmp);
        serialize(entry, tmp);
    }
    assert(tmp == res.data() + res.size());
    return std::make_pair(std::move(res), Status::DONE);
}
