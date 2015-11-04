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
#include "Transactions.hpp"

namespace aim {


Q1Out Transactions::q1Transaction(tell::db::Transaction& tx, const Q1In& in)
{
    auto wFuture = tx.openTable("wt");
    auto wideTable = wFuture.get();
    // TODO: implement
    Q1Out res;
    return res;
//    void
//    Query1ServerObject::processBucket(uint thread_id, const BucketReference &bucket)
//    {
//        assert(bucket.num_of_records == RECORDS_PER_BUCKET);
//        auto avg = q1_simple<RECORDS_PER_BUCKET>(_calls_sum_local_week.get(bucket), _alpha,
//                                                 _dur_sum_all_week.get(bucket));
//        _sums[thread_id] += avg.sum;
//        _counts[thread_id] += avg.count;
//    }

//    void
//    Query1ServerObject::processLastBucket(uint thread_id, const BucketReference &bucket)
//    {
//        auto avg = q1_simple(_calls_sum_local_week.get(bucket), _alpha,
//                             _dur_sum_all_week.get(bucket),
//                             bucket.num_of_records);

//        _sums[thread_id] += avg.sum;
//        _counts[thread_id] += avg.count;
//    }

//    auto
//    Query1ServerObject::popResult() -> std::pair<std::vector<char>, Status>
//    {
//        uint64_t sum = std::accumulate(_sums.begin(), _sums.end(), uint64_t(0));
//        uint32_t count = std::accumulate(_counts.begin(), _counts.end(), uint32_t(0));
//        std::vector<char> resultBuf(sizeof(sum) + sizeof(count));
//        *(reinterpret_cast<decltype(sum) *>(resultBuf.data())) = sum;
//        *(reinterpret_cast<decltype(count) *>(&resultBuf[sizeof(sum)])) = count;
//        return std::make_pair(std::move(resultBuf), Status::DONE);
//    }
}

Q2Out Transactions::q2Transaction(tell::db::Transaction& tx, const Q2In& in)
{
    auto wFuture = tx.openTable("wt");
    auto wideTable = wFuture.get();
    // TODO: implement
    Q2Out res;
    return res;
//    void
//    Query2ServerObject::processBucket(uint thread_id, const BucketReference &bucket)
//    {
//        assert(bucket.num_of_records == RECORDS_PER_BUCKET);
//        auto result = q2_simd<RECORDS_PER_BUCKET>(_cost_max_all_week.get(bucket),
//                                                  _calls_sum_all_week.get(bucket), _alpha);
//        _maxs[thread_id] = std::max(_maxs[thread_id], result);
//    }

//    void
//    Query2ServerObject::processLastBucket(uint thread_id, const BucketReference &bucket)
//    {
//        auto result = q2_simple(_cost_max_all_week.get(bucket),
//                                _calls_sum_all_week.get(bucket),
//                                _alpha, bucket.num_of_records);
//        _maxs[thread_id] = std::max(_maxs[thread_id], result);
//    }

//    auto
//    Query2ServerObject::popResult() -> std::pair<std::vector<char>, Status>
//    {
//        double result = *std::max_element(_maxs.begin(), _maxs.end());
//        std::vector<char> resultBuf(sizeof(result));
//        *(reinterpret_cast<decltype(result)*>(resultBuf.data())) = result;
//        return std::make_pair(std::move(resultBuf), Status::DONE);
//    }
}

Q3Out q3Transaction(tell::db::Transaction& tx)
{
    auto wFuture = tx.openTable("wt");
    auto wideTable = wFuture.get();
    // TODO: implement
    Q3Out res;
    return res;
//    void
//    Query3ServerObject::processBucket(uint thread_id, const BucketReference &bucket)
//    {
//        assert(bucket.num_of_records == RECORDS_PER_BUCKET);
//        q3_simple<RECORDS_PER_BUCKET>(_cost_sum_all_week.get(bucket),
//                                      _dur_sum_all_week.get(bucket),
//                                      _calls_sum_all_week.get(bucket),
//                                      _active_results[thread_id]);
//    }

//    void
//    Query3ServerObject::processLastBucket(uint thread_id, const BucketReference &bucket)
//    {
//        q3_simple(_cost_sum_all_week.get(bucket),
//                  _dur_sum_all_week.get(bucket),
//                  _calls_sum_all_week.get(bucket),
//                  bucket.num_of_records, _active_results[thread_id]);
//    }

//    auto
//    Query3ServerObject::popResult() -> std::pair<std::vector<char>, Status>
//    {
//        Q3Result result;
//        for (Q3Result& partial_result : _active_results) {
//            for (uint i=0; i<Q3Result::SMALL_THRESHOLD; ++i) {
//                result.small_values[i] += partial_result.small_values[i];
//            }
//            for (auto iter = partial_result.outliers.begin(); iter != partial_result.outliers.end(); ++iter) {
//                result.outliers[iter->first] += iter->second;
//            }
//        }
//        auto number_of_result_tuples = result.outliers.size();
//        for (uint i=0; i<Q3Result::SMALL_THRESHOLD; ++i) {
//            if (result.small_values[i].cost_sum_all_week) {
//                ++number_of_result_tuples;
//            }
//        }
//        size_t result_size = sizeof(number_of_result_tuples) + number_of_result_tuples * sizeof(Q3Tuple);
//        std::vector<char> resultBuf(result_size);
//        char* tmp = resultBuf.data();
//        memcpy(tmp, &number_of_result_tuples, sizeof(number_of_result_tuples));
//        tmp += sizeof(number_of_result_tuples);
//        for (uint32_t i=0; i<Q3Result::SMALL_THRESHOLD; ++i) {
//            if (result.small_values[i].cost_sum_all_week) {
//                memcpy(tmp, &result.small_values[i], sizeof(Q3Tuple));
//                tmp += sizeof(Q3Tuple);
//            }
//        }

//        for (auto iter = result.outliers.begin(); iter != result.outliers.end(); ++iter) {
//            memcpy(tmp, &iter->second, sizeof(Q3Tuple));
//            tmp += sizeof(Q3Tuple);
//        }
//        assert(tmp == resultBuf.data() + resultBuf.size());
//        return std::make_pair(std::move(resultBuf), Status::DONE);
//    }
}

Q4Out q4Transaction(tell::db::Transaction& tx, const Q4In& in)
{
    auto wFuture = tx.openTable("wt");
    auto wideTable = wFuture.get();
    // TODO: implement
    Q4Out res;
    return res;
//    void
//    Query4ServerObject::processBucket(uint thread_id, const BucketReference& bucket)
//    {
//        assert(bucket.num_of_records == RECORDS_PER_BUCKET);
//        //q4_simple creates a Q4Entry (avg_sum, avg_cnt, sum) for every city
//        q4_simple<RECORDS_PER_BUCKET>(_calls_sum_local_week.get(bucket), _alpha,
//                                      _dur_sum_local_week.get(bucket), _beta,
//                                      _region_city.get(bucket), _entries);
//    }

//    void
//    Query4ServerObject::processLastBucket(uint thread_id, const BucketReference& bucket)
//    {
//        q4_simple(_calls_sum_local_week.get(bucket), _alpha,
//                  _dur_sum_local_week.get(bucket), _beta,
//                  _region_city.get(bucket), bucket.num_of_records, _entries);
//    }

//    auto
//    Query4ServerObject::popResult() -> std::pair<std::vector<char>, Status>
//    {
//        size_t res_size = region_unique_city.size() * (sizeof(size_t) + sizeof(CityEntry));
//        for (std::string &city: region_unique_city) {
//            res_size += city.size();
//        }
//        std::vector<char> res(res_size);
//        char *tmp = res.data();
//        for (size_t i=0; i<_entries.size(); ++i) {

//            CityEntry &entry = _entries[i];
//            serialize(region_unique_city[i], tmp);
//            memcpy(tmp, &entry, sizeof(CityEntry));
//            tmp += sizeof(CityEntry);
//        }
//        assert(tmp == res.data() + res.size());
//        return std::make_pair(std::move(res), Status::DONE);
//    }
}

Q5Out q5Transaction(tell::db::Transaction& tx, const Q5In& in)
{
    auto wFuture = tx.openTable("wt");
    auto wideTable = wFuture.get();
    // TODO: implement
    Q5Out res;
    return res;
//    void
//    Query5ServerObject::processBucket(uint thread_id, const BucketReference& bucket)
//    {
//        assert(bucket.num_of_records == RECORDS_PER_BUCKET);
//        q5_simple<RECORDS_PER_BUCKET>(_region_region.get(bucket),
//                                      _cost_sum_local_week.get(bucket),
//                                      _cost_sum_long_week.get(bucket),
//                                      _subscription_type.get(bucket),
//                                      _subscriber_category.get(bucket),
//                                      _type, _category, _entries[thread_id]);
//    }

//    void
//    Query5ServerObject::processLastBucket(uint thread_id, const BucketReference& bucket)
//    {
//        q5_simple(_region_region.get(bucket),
//                  _cost_sum_local_week.get(bucket),
//                  _cost_sum_long_week.get(bucket),
//                  _subscription_type.get(bucket),
//                  _subscriber_category.get(bucket),
//                  _type, _category, bucket.num_of_records,
//                  _entries[thread_id]);
//    }

//    auto
//    Query5ServerObject::popResult() -> std::pair<std::vector<char>, Status>
//    {
//        std::vector<RegionEntry> regions(region_unique_region.size());
//        for (auto& thread_entries : _entries) {
//            assert(thread_entries.size() == regions.size());
//            for (size_t i=0; i<thread_entries.size(); ++i) {
//                regions[i].local_sum += thread_entries[i].local_sum;
//                regions[i].long_sum += thread_entries[i].long_sum;
//            }

//        }
//        size_t res_size = region_unique_region.size() * (2 * sizeof(double) + sizeof(size_t));
//        for (std::string &region: region_unique_region) {
//            res_size += region.size();
//        }
//        std::vector<char> res(res_size);
//        char *tmp = res.data();
//        for (size_t i=0; i<regions.size(); ++i) {
//            RegionEntry &entry = regions[i];
//            serialize(region_unique_region[i], tmp);
//            serialize(entry, tmp);
//        }
//        assert(tmp == res.data() + res.size());
//        return std::make_pair(std::move(res), Status::DONE);
//    }
}

Q6Out q6Transaction(tell::db::Transaction& tx, const Q6In& in)
{
    auto wFuture = tx.openTable("wt");
    auto wideTable = wFuture.get();
    // TODO: implement
    Q6Out res;
    return res;
//    void
//    Query6ServerObject::processBucket(uint thread_id, const BucketReference& bucket)
//    {
//        assert(bucket.num_of_records == RECORDS_PER_BUCKET);
//        q6_simple(_region_country.get(bucket),
//                  _country_id,
//                  _subscriber_id.get(bucket),
//                  _dur_max_local_week.get(bucket),
//                  _dur_max_local_day.get(bucket),
//                  _dur_max_long_week.get(bucket),
//                  _dur_max_long_day.get(bucket),
//                  RECORDS_PER_BUCKET, _entries[thread_id]);
//    }

//    void
//    Query6ServerObject::processLastBucket(uint thread_id, const BucketReference& bucket)
//    {
//        q6_simple(_region_country.get(bucket),
//                  _country_id,
//                  _subscriber_id.get(bucket),
//                  _dur_max_local_week.get(bucket),
//                  _dur_max_local_day.get(bucket),
//                  _dur_max_long_week.get(bucket),
//                  _dur_max_long_day.get(bucket),
//                  bucket.num_of_records, _entries[thread_id]);
//    }

//    auto
//    Query6ServerObject::popResult() -> std::pair<std::vector<char>, Status>
//    {
//        Query6Result q6_res;
//        for (Query6Result& thread_result: _entries) {
//            if (q6_res.max_local_day < thread_result.max_local_day) {
//                q6_res.max_local_day = thread_result.max_local_day;
//                q6_res.max_local_day_id = thread_result.max_local_day_id;
//            }
//            if (q6_res.max_local_week < thread_result.max_local_week) {
//                q6_res.max_local_week = thread_result.max_local_week;
//                q6_res.max_local_week_id = thread_result.max_local_week_id;
//            }
//            if (q6_res.max_long_day < thread_result.max_long_day) {
//                q6_res.max_long_day = thread_result.max_long_day;
//                q6_res.max_long_day_id = thread_result.max_long_day_id;
//            }
//            if (q6_res.max_long_week < thread_result.max_long_week) {
//                q6_res.max_long_week = thread_result.max_long_week;
//                q6_res.max_long_week_id = thread_result.max_long_week_id;
//            }
//        }
//        std::vector<char> res(sizeof(Query6Result));
//        std::memcpy(res.data(), &q6_res, sizeof(q6_res));
//        return std::make_pair(std::move(res), Status::DONE);
//    }
}

Q7Out q7Transaction(tell::db::Transaction& tx, const Q7In& in)
{
    auto wFuture = tx.openTable("wt");
    auto wideTable = wFuture.get();
    // TODO: implement
    Q7Out res;
    return res;

//    void
//    Query7ServerObject::processBucket(uint thread_id, const BucketReference& bucket)
//    {
//        assert(bucket.num_of_records == RECORDS_PER_BUCKET);
//        const double* cost_sum;
//        const uint32_t* duration_sum;
//        if (_window_length == WindowLength::DAY) {
//            cost_sum = _cost_sum_all_day.get(bucket);
//            duration_sum = _dur_sum_all_day.get(bucket);
//        }
//        else {
//            cost_sum = _cost_sum_all_week.get(bucket);
//            duration_sum = _dur_sum_all_week.get(bucket);
//        }
//        q7_simple(_subscriber_value_type.get(bucket),
//                  _value_type_id, _subscriber_id.get(bucket),
//                  cost_sum, duration_sum,
//                  RECORDS_PER_BUCKET, _entries[thread_id]);
//    }

//    void
//    Query7ServerObject::processLastBucket(uint thread_id, const BucketReference& bucket)
//    {
//        const double* cost_sum;
//        const uint32_t* duration_sum;
//        if (_window_length == WindowLength::DAY) {
//            cost_sum = _cost_sum_all_day.get(bucket);
//            duration_sum = _dur_sum_all_day.get(bucket);
//        }
//        else {
//            cost_sum = _cost_sum_all_week.get(bucket);
//            duration_sum = _dur_sum_all_week.get(bucket);
//        }
//        q7_simple(_subscriber_value_type.get(bucket),
//                  _value_type_id, _subscriber_id.get(bucket),
//                  cost_sum, duration_sum,
//                  bucket.num_of_records, _entries[thread_id]);
//    }

//    auto
//    Query7ServerObject::popResult() -> std::pair<std::vector<char>, Status>
//    {
//        Query7Result q7_res;
//        for (Query7Result& thread_result: _entries) {
//            if (q7_res.avg_money_per_time > thread_result.avg_money_per_time) {
//                q7_res.avg_money_per_time = thread_result.avg_money_per_time;
//                q7_res.subscriber_id = thread_result.subscriber_id;
//            }
//        }
//        std::vector<char> res(sizeof(Query7Result));
//        std::memcpy(res.data(), &q7_res, sizeof(q7_res));
//        return std::make_pair(std::move(res), Status::DONE);
//    }
}

} // namespace aim

