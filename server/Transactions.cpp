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

#include <crossbow/enum_underlying.hpp>

#include <tellstore/Table.hpp>
#include <tellstore/ScanMemory.hpp>

#include <map>

#include "Connection.hpp"

#include <common/dimension-tables-unique-values.h>

namespace aim {

using namespace tell::db;
using namespace tell::store;

Q1Out Transactions::q1Transaction(Transaction& tx, Context &context, const Q1In& in)
{
    Q1Out result;

    try {
        auto wFuture = tx.openTable("wt");
        auto wideTable = wFuture.get();
        auto schema = tx.getSchema(wideTable);

        uint32_t selectionLength = 24;
        std::unique_ptr<char[]> selection(new char[selectionLength]);

        crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
        selectionWriter.write<uint64_t>(0x1u);
        selectionWriter.write<uint16_t>(context.callsSumLocalWeek);
        selectionWriter.write<uint16_t>(0x1u);
        selectionWriter.align(sizeof(uint64_t));
        selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER));
        selectionWriter.write<uint8_t>(0x0u);
        selectionWriter.align(sizeof(uint32_t));
        selectionWriter.write<int32_t>(in.alpha);

        uint32_t aggregationLength = 8;
        std::unique_ptr<char[]> aggregation(new char[aggregationLength]);

        crossbow::buffer_writer aggregationWriter(aggregation.get(), aggregationLength);
        aggregationWriter.write<uint16_t>(context.durSumAllWeek);
        aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::SUM));
        aggregationWriter.write<uint16_t>(context.durSumAllWeek);
        aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::CNT));

        Schema resultSchema(schema.type());
        resultSchema.addField(FieldType::BIGINT, "sum", true);
        resultSchema.addField(FieldType::INT, "cnt", true);
        Table resultTable(wideTable.value, std::move(resultSchema));

        auto &snapshot = tx.snapshot();
        auto &clientHandle = tx.getHandle();
        auto scanIterator = clientHandle.scan(resultTable, snapshot,
                *context.scanMemoryMananger, ScanQueryType::AGGREGATION, selectionLength,
                selection.get(), aggregationLength, aggregation.get());

        if (scanIterator->hasNext()) {
            const char* tuple;
            size_t tupleLength;
            std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
            result.avg = resultTable.field<int64_t>("sum", tuple);
            int32_t cnt = resultTable.field<int32_t>("cnt", tuple);
            if (cnt != 0)   // don-t divide by zero, report 0!
                result.avg /= cnt;
            result.success = true;
        }

        if (scanIterator->error()) {
            result.error = crossbow::to_string(scanIterator->error().value());
            result.success = false;
        }

        tx.commit();
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

Q2Out Transactions::q2Transaction(Transaction& tx, Context &context, const Q2In& in)
{
    Q2Out result;

    try {
        auto wFuture = tx.openTable("wt");
        auto wideTable = wFuture.get();
        auto schema = tx.getSchema(wideTable);

        uint32_t selectionLength = 24;
        std::unique_ptr<char[]> selection(new char[selectionLength]);

        crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
        selectionWriter.write<uint64_t>(0x1u);
        selectionWriter.write<uint16_t>(context.callsSumAllWeek);
        selectionWriter.write<uint16_t>(0x1u);
        selectionWriter.align(sizeof(uint64_t));
        selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER));
        selectionWriter.write<uint8_t>(0x0u);
        selectionWriter.align(sizeof(uint32_t));
        selectionWriter.write<int32_t>(in.alpha);

        uint32_t aggregationLength = 4;
        std::unique_ptr<char[]> aggregation(new char[aggregationLength]);

        crossbow::buffer_writer aggregationWriter(aggregation.get(), aggregationLength);
        aggregationWriter.write<uint16_t>(context.costMaxAllWeek);
        aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::MAX));

        Schema resultSchema(schema.type());
        resultSchema.addField(FieldType::DOUBLE, "max", true);
        Table resultTable(wideTable.value, std::move(resultSchema));

        auto &snapshot = tx.snapshot();
        auto &clientHandle = tx.getHandle();
        auto scanIterator = clientHandle.scan(resultTable, snapshot,
                *context.scanMemoryMananger, ScanQueryType::AGGREGATION, selectionLength,
                selection.get(), aggregationLength, aggregation.get());

        if (scanIterator->hasNext()) {
            const char* tuple;
            size_t tupleLength;
            std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
            result.max = resultTable.field<double>("max", tuple);
            result.success = true;
        }

        if (scanIterator->error()) {
            result.error = crossbow::to_string(scanIterator->error().value());
            result.success = false;
        }

        tx.commit();
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

Q3Out Transactions::q3Transaction(Transaction& tx, Context &context)
{
    Q3Out result;

    try {
        auto wFuture = tx.openTable("wt");
        auto wideTable = wFuture.get();
        auto schema = tx.getSchema(wideTable);

        // idea: we have to group by callsSumAllWeek
        // --> allmost all values in [0,10) --> create 10 parallel aggregations
        // for values 0, 1, ...., 9
        // create a projection on >= 10 and aggregate manually

        uint32_t selectionLength = 24;
        std::unique_ptr<char[]> selection(new char[selectionLength]);

        crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
        selectionWriter.write<uint64_t>(0x1u);
        selectionWriter.write<uint16_t>(context.callsSumAllWeek);
        selectionWriter.write<uint16_t>(0x1u);
        selectionWriter.align(sizeof(uint64_t));
        selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::EQUAL));
        selectionWriter.write<uint8_t>(0x0u);
        selectionWriter.align(sizeof(uint32_t));
        selectionWriter.write<int32_t>(0);      // we are going to vary this

        // sort projection attributes
        std::map<id_t, std::tuple<AggregationType, FieldType,
                crossbow::string>> projectionAttributes;
        projectionAttributes[context.costSumAllWeek] = std::make_tuple(
                AggregationType::SUM, FieldType::DOUBLE, "cost_sum_all_week");
        projectionAttributes[context.durSumAllWeek] = std::make_tuple(
                AggregationType::SUM, FieldType::BIGINT, "dur_sum_all_week");

        Schema resultSchema(schema.type());

        uint32_t aggregationLength = 4 * projectionAttributes.size();
        std::unique_ptr<char[]> aggregation(new char[aggregationLength]);

        crossbow::buffer_writer aggregationWriter(aggregation.get(), aggregationLength);
        for (auto &attribute : projectionAttributes) {
            aggregationWriter.write<uint16_t>(attribute.first);
            aggregationWriter.write<uint16_t>(
                    crossbow::to_underlying(std::get<0>(attribute.second)));
            resultSchema.addField(std::get<1>(attribute.second),
                    std::get<2>(attribute.second), true);
        }

        Table resultTable(wideTable.value, std::move(resultSchema));

        auto &snapshot = tx.snapshot();
        auto &clientHandle = tx.getHandle();
        std::vector<std::shared_ptr<ScanIterator>> scanIterators;
        scanIterators.reserve(11);
        for (int32_t i = 0; i < 10; ++i)
        {
            *(reinterpret_cast<int32_t*>(&selection[20])) = i;
            scanIterators.push_back(clientHandle.scan(resultTable, snapshot,
                    *context.scanMemoryMananger, ScanQueryType::AGGREGATION, selectionLength,
                    selection.get(), aggregationLength, aggregation.get()));
        }

        // projection on rest
        *(reinterpret_cast<uint8_t*>(&selection[16])) =
                crossbow::to_underlying(PredicateType::GREATER_EQUAL);
        *(reinterpret_cast<int32_t*>(&selection[20])) = 10;

        // add one more projection attributes for the non-aggregation scan
        projectionAttributes[context.callsSumAllWeek] = std::make_tuple(
                AggregationType::SUM, FieldType::INT, "calls_sum_all_week");
        uint32_t projectionLength = 2*projectionAttributes.size();
        std::unique_ptr<char[]> projection(new char[projectionLength]);

        Schema projectionResultSchema(schema.type());

        crossbow::buffer_writer projectionWriter(projection.get(), projectionLength);
        for (auto &attribute : projectionAttributes) {
            projectionWriter.write<uint16_t>(attribute.first);
            projectionResultSchema.addField(std::get<1>(attribute.second),
                    std::get<2>(attribute.second), true);
        }

        Table projectionResultTable(wideTable.value, std::move(projectionResultSchema));

        // run the rest-projection scan
        scanIterators.push_back(clientHandle.scan(projectionResultTable, snapshot,
                *context.scanMemoryMananger, ScanQueryType::PROJECTION, selectionLength,
                selection.get(), projectionLength, projection.get()));

        // process results from aggregations
        for (int32_t i = 0; i < 10; ++i)
        {
            auto &scanIterator = scanIterators[i];
            if (scanIterator->hasNext()) {
                const char* tuple;
                size_t tupleLength;
                std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
                double costSumAllWeek = resultTable.field<double>(
                        "cost_sum_all_week", tuple);
                int64_t durSumAllWeek = resultTable.field<int64_t>(
                        "dur_sum_all_week", tuple);
                if (durSumAllWeek > 0) {
                    Q3Out::Q3Tuple q3Tuple;
                    q3Tuple.number_of_calls_this_week = i;
                    q3Tuple.cost_ratio = costSumAllWeek / durSumAllWeek;
                    result.results.push_back(std::move(q3Tuple));
                }
            }
        }

        // process rest query (in a proper scope)
        {
            auto &scanIterator = scanIterators[10];
            std::map<uint32_t, double> costs;
            std::map<uint32_t, int64_t> durations;
            while (scanIterator->hasNext()) {
                const char* tuple;
                size_t tupleLength;
                std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
                double costSumAllWeek = projectionResultTable.field<double>(
                        "cost_sum_all_week", tuple);
                int64_t durSumAllWeek = projectionResultTable.field<int64_t>(
                        "dur_sum_all_week", tuple);
                int32_t callsSumAllWeek  = projectionResultTable.field<double>(
                            "calls_sum_all_week", tuple);
                if (costs.find(callsSumAllWeek) == costs.end()) {
                    // entry does not exists yet, create it
                    costs[callsSumAllWeek] = 0.0;
                    durations[callsSumAllWeek] = 0;
                }
                costs[callsSumAllWeek] += costSumAllWeek;
                durations[callsSumAllWeek] += durSumAllWeek;

                for (auto &costEntry : costs) {
                    Q3Out::Q3Tuple q3Tuple;
                    q3Tuple.number_of_calls_this_week = costEntry.first;
                    q3Tuple.cost_ratio = costEntry.second
                            / durations[costEntry.first];
                    result.results.push_back(std::move(q3Tuple));
                }
            }
        }

        result.success = true;
        tx.commit();

        for (auto &scanIterator: scanIterators) {
            if (scanIterator->error()) {
                result.error = crossbow::to_string(scanIterator->error().value());
                result.success = false;
                return result;
            }

        }
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

Q4Out Transactions::q4Transaction(Transaction& tx, Context &context, const Q4In& in)
{
    Q4Out result;

    try {
        auto wFuture = tx.openTable("wt");
        auto wideTable = wFuture.get();
        auto schema = tx.getSchema(wideTable);

        // idea: we have to group by cityName
        // 5 different unique values
        // aggregate them all in separate scans

        uint32_t selectionLength = 56;
        std::unique_ptr<char[]> selection(new char[selectionLength]);

        crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
        selectionWriter.write<uint64_t>(0x3u);

        selectionWriter.write<uint16_t>(context.regionCity);
        selectionWriter.write<uint16_t>(0x1u);
        selectionWriter.align(sizeof(uint64_t));
        selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::EQUAL));
        selectionWriter.write<uint8_t>(0x0u);
        selectionWriter.align(sizeof(uint32_t));
        selectionWriter.write<int32_t>(0);                // we are going to vary this

        selectionWriter.write<uint16_t>(context.callsSumLocalWeek);
        selectionWriter.write<uint16_t>(0x1u);
        selectionWriter.align(sizeof(uint64_t));
        selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER));
        selectionWriter.write<uint8_t>(0x0u);
        selectionWriter.align(sizeof(uint32_t));
        selectionWriter.write<int32_t>(in.alpha);

        selectionWriter.write<uint16_t>(context.durSumLocalWeek);
        selectionWriter.write<uint16_t>(0x1u);
        selectionWriter.align(sizeof(uint64_t));
        selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER));
        selectionWriter.write<uint8_t>(0x0u);
        selectionWriter.align(sizeof(uint32_t));
        selectionWriter.write<int32_t>(in.beta);


        // sort projection attributes
        std::map<id_t, std::tuple<AggregationType, FieldType,
                crossbow::string>> aggregationAttributes;
        aggregationAttributes[context.callsSumLocalWeek] = std::make_tuple(
                AggregationType::SUM, FieldType::INT, "sum_calls_sum_local_week");
        aggregationAttributes[context.callsSumLocalWeek] = std::make_tuple(
                AggregationType::CNT, FieldType::INT, "cnt_calls_sum_local_week");
        aggregationAttributes[context.durSumLocalWeek] = std::make_tuple(
                AggregationType::SUM, FieldType::BIGINT, "dur_sum_local_week");

        Schema resultSchema(schema.type());

        uint32_t aggregationLength = 4 * aggregationAttributes.size();
        std::unique_ptr<char[]> aggregation(new char[aggregationLength]);

        crossbow::buffer_writer aggregationWriter(aggregation.get(), aggregationLength);
        for (auto &attribute : aggregationAttributes) {
            aggregationWriter.write<uint16_t>(attribute.first);
            aggregationWriter.write<uint16_t>(
                    crossbow::to_underlying(std::get<0>(attribute.second)));
            resultSchema.addField(std::get<1>(attribute.second),
                    std::get<2>(attribute.second), true);
        }

        Table resultTable(wideTable.value, std::move(resultSchema));

        auto &snapshot = tx.snapshot();
        auto &clientHandle = tx.getHandle();
        std::vector<std::shared_ptr<ScanIterator>> scanIterators;
        uint16_t numberOfCities = region_unique_city.size();
        scanIterators.reserve(numberOfCities);
        for (int32_t i = 0; i < numberOfCities; ++i)
        {
            *(reinterpret_cast<int32_t*>(&selection[20])) = i;
            scanIterators.push_back(clientHandle.scan(resultTable, snapshot,
                    *context.scanMemoryMananger, ScanQueryType::AGGREGATION, selectionLength,
                    selection.get(), aggregationLength, aggregation.get()));
        }

        // process results from aggregations
        for (int16_t i = 0; i < numberOfCities; ++i)
        {
            auto &scanIterator = scanIterators[i];
            if (scanIterator->hasNext()) {
                const char* tuple;
                size_t tupleLength;
                std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
                int32_t sumCallsSumLocalWeek = resultTable.field<int32_t>(
                        "sum_calls_sum_local_week", tuple);
                int32_t cntCallsSumLocalWeek = resultTable.field<int32_t>(
                        "cnt_calls_sum_local_week", tuple);
                int64_t durSumLocalWeek = resultTable.field<int64_t>(
                        "dur_sum_local_week", tuple);
                if (cntCallsSumLocalWeek > 0) {
                    Q4Out::Q4Tuple q4Tuple;
                    q4Tuple.city_name = region_unique_city[i];
                    q4Tuple.avg_num_local_calls_week =
                            static_cast<double>(sumCallsSumLocalWeek)
                                    / cntCallsSumLocalWeek;
                    q4Tuple.sum_duration_local_calls_week = durSumLocalWeek;
                    result.results.push_back(std::move(q4Tuple));
                }
            }
        }

        result.success = true;
        tx.commit();

        for (auto &scanIterator: scanIterators) {
            if (scanIterator->error()) {
                result.error = crossbow::to_string(scanIterator->error().value());
                result.success = false;
                return result;
            }

        }
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

Q5Out Transactions::q5Transaction(Transaction& tx, Context &context, const Q5In& in)
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

Q6Out Transactions::q6Transaction(Transaction& tx, Context &context, const Q6In& in)
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

Q7Out Transactions::q7Transaction(Transaction& tx, Context &context, const Q7In& in)
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

