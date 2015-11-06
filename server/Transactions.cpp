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


        // sort aggregation attributes
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
        for (int16_t i = 0; i < numberOfCities; ++i)
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
    Q5Out result;

    try {
        auto wFuture = tx.openTable("wt");
        auto wideTable = wFuture.get();
        auto schema = tx.getSchema(wideTable);

        // idea: we have to group by cityName
        // 5 different unique values
        // aggregate them all in separate scans

        uint32_t selectionLength = 24;
        std::unique_ptr<char[]> selection(new char[selectionLength]);

        crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
        selectionWriter.write<uint64_t>(0x1u);

        selectionWriter.write<uint16_t>(context.regionRegion);
        selectionWriter.write<uint16_t>(0x1u);
        selectionWriter.align(sizeof(uint64_t));
        selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::EQUAL));
        selectionWriter.write<uint8_t>(0x0u);
        selectionWriter.align(sizeof(uint32_t));
        selectionWriter.write<int32_t>(0);                // we are going to vary this

        // sort aggregation attributes
        std::map<id_t, std::tuple<AggregationType, FieldType,
                crossbow::string>> aggregationAttributes;
        aggregationAttributes[context.costSumLocalWeek] = std::make_tuple(
                AggregationType::SUM, FieldType::DOUBLE, "sum_cost_sum_local_week");
        aggregationAttributes[context.costSumDistantWeek] = std::make_tuple(
                AggregationType::SUM, FieldType::DOUBLE, "sum_cost_sum_distant_week");

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
        uint16_t numberOfRegions = region_unique_region.size();
        scanIterators.reserve(numberOfRegions);
        for (int16_t i = 0; i < numberOfRegions; ++i)
        {
            *(reinterpret_cast<int32_t*>(&selection[20])) = i;
            scanIterators.push_back(clientHandle.scan(resultTable, snapshot,
                    *context.scanMemoryMananger, ScanQueryType::AGGREGATION, selectionLength,
                    selection.get(), aggregationLength, aggregation.get()));
        }

        // process results from aggregations
        for (int16_t i = 0; i < numberOfRegions; ++i)
        {
            auto &scanIterator = scanIterators[i];
            if (scanIterator->hasNext()) {
                const char* tuple;
                size_t tupleLength;
                std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
                double sumCostSumLocalWeek = resultTable.field<double>(
                        "sum_cost_sum_local_week", tuple);
                double sumCostSumDistantWeek = resultTable.field<double>(
                        "sum_cost_sum_distant_week", tuple);
                if (sumCostSumLocalWeek > 0.0) {
                    Q5Out::Q5Tuple q5Tuple;
                    q5Tuple.region_name = region_unique_region[i];
                    q5Tuple.sum_cost_local_calls_week = sumCostSumLocalWeek;
                    q5Tuple.sum_cost_longdistance_calls_week = sumCostSumDistantWeek;
                    result.results.push_back(std::move(q5Tuple));
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

Q6Out Transactions::q6Transaction(Transaction& tx, Context &context, const Q6In& in)
{
    Q6Out result;

    try {
        auto wFuture = tx.openTable("wt");
        auto wideTable = wFuture.get();
        auto schema = tx.getSchema(wideTable);
        auto &snapshot = tx.snapshot();
        auto &clientHandle = tx.getHandle();

        {   // find the minima / maxima

            uint32_t selectionLength = 24;
            std::unique_ptr<char[]> selection(new char[selectionLength]);

            crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
            selectionWriter.write<uint64_t>(0x1u);
            selectionWriter.write<uint16_t>(context.regionCountry);
            selectionWriter.write<uint16_t>(0x1u);
            selectionWriter.align(sizeof(uint64_t));
            selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::EQUAL));
            selectionWriter.write<uint8_t>(0x0u);
            selectionWriter.align(sizeof(uint32_t));
            selectionWriter.write<int32_t>(in.country_id);

            // sort aggregation attributes
            std::map<id_t, std::tuple<AggregationType, FieldType,
                    crossbow::string>> aggregationAttributes;
            aggregationAttributes[context.durMaxLocalWeek] = std::make_tuple(
                    AggregationType::MAX, FieldType::DOUBLE, "max_local_week");
            aggregationAttributes[context.durMaxLocalDay] = std::make_tuple(
                    AggregationType::MAX, FieldType::DOUBLE, "max_local_day");
            aggregationAttributes[context.durMaxDistantWeek] = std::make_tuple(
                    AggregationType::MAX, FieldType::DOUBLE, "max_distant_week");
            aggregationAttributes[context.durMaxDistantDay] = std::make_tuple(
                    AggregationType::MAX, FieldType::DOUBLE, "max_distant_day");

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

            auto scanIterator = clientHandle.scan(resultTable, snapshot,
                    *context.scanMemoryMananger, ScanQueryType::AGGREGATION, selectionLength,
                    selection.get(), aggregationLength, aggregation.get());

            if (scanIterator->hasNext()) {
                const char* tuple;
                size_t tupleLength;
                std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
                result.max_local_week = resultTable.field<int64_t>("max_local_week", tuple);
                result.max_local_day = resultTable.field<int64_t>("max_local_day", tuple);
                result.max_distant_week = resultTable.field<int64_t>("max_distant_week", tuple);
                result.max_distant_day = resultTable.field<int64_t>("max_distant_day", tuple);
            }

            if (scanIterator->error()) {
                result.error = crossbow::to_string(scanIterator->error().value());
                result.success = false;
                return result;
            }
        }

        {   // find the corresponding IDs with 4 parallel scans

            uint32_t selectionLength = 24;
            std::unique_ptr<char[]> selection(new char[selectionLength]);

            crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
            selectionWriter.write<uint64_t>(0x1u);
            selectionWriter.write<uint16_t>(context.durMaxLocalWeek);   // we are going to vary this
            selectionWriter.write<uint16_t>(0x1u);
            selectionWriter.align(sizeof(uint64_t));
            selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::EQUAL));
            selectionWriter.write<uint8_t>(0x0u);
            selectionWriter.align(sizeof(uint32_t));
            selectionWriter.write<int32_t>(result.max_local_week);      // we are going to vary this

            std::vector<std::pair<id_t, int64_t>> selectionValues;
            selectionValues.reserve(4);
            selectionValues.emplace_back(context.durMaxLocalWeek, result.max_local_week);
            selectionValues.emplace_back(context.durMaxLocalDay, result.max_local_day);
            selectionValues.emplace_back(context.durMaxDistantWeek, result.max_distant_week);
            selectionValues.emplace_back(context.durMaxDistantDay, result.max_distant_day);


            uint32_t aggregationLength = 4;
            std::unique_ptr<char[]> aggregation(new char[aggregationLength]);
            crossbow::buffer_writer aggregationWriter(aggregation.get(), aggregationLength);
            aggregationWriter.write<uint16_t>(context.subscriberId);
            aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::MIN));

            Schema resultSchema(schema.type());
            resultSchema.addField(FieldType::BIGINT, "min_subscriber_id", true);
            Table resultTable(wideTable.value, std::move(resultSchema));

            std::vector<std::shared_ptr<ScanIterator>> scanIterators;
            scanIterators.reserve(4);
            for (uint i = 0; i < 4; ++i) {
                *(reinterpret_cast<int16_t*>(&selection[8])) = selectionValues[i].first;
                *(reinterpret_cast<int32_t*>(&selection[20])) = selectionValues[i].second;
                scanIterators.push_back(
                        clientHandle.scan(resultTable, snapshot,
                        *context.scanMemoryMananger, ScanQueryType::AGGREGATION, selectionLength,
                        selection.get(), aggregationLength, aggregation.get()));
            }

            // query the results
            const char* tuple;
            size_t tupleLength;

            auto &scanIterator = scanIterators[0];
            if (scanIterator->hasNext()) {
                std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
                result.max_local_week_id = resultTable.field<int64_t>("min_subscriber_id", tuple);
            }

            scanIterator = scanIterators[1];
            if (scanIterator->hasNext()) {
                std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
                result.max_local_day_id = resultTable.field<int64_t>("min_subscriber_id", tuple);
            }

            scanIterator = scanIterators[2];
            if (scanIterator->hasNext()) {
                std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
                result.max_distant_week_id = resultTable.field<int64_t>("min_subscriber_id", tuple);
            }

            scanIterator = scanIterators[3];
            if (scanIterator->hasNext()) {
                std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
                result.max_distant_day_id = resultTable.field<int64_t>("min_subscriber_id", tuple);
            }

            // check for errors
            for (auto &scanIterator: scanIterators) {
                if (scanIterator->error()) {
                    result.error = crossbow::to_string(scanIterator->error().value());
                    result.success = false;
                    return result;
                }
            }
        }

        result.success = true;
        tx.commit();

    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

Q7Out Transactions::q7Transaction(Transaction& tx, Context &context, const Q7In& in)
{
    Q7Out result;

    try {
        auto wFuture = tx.openTable("wt");
        auto wideTable = wFuture.get();
        auto schema = tx.getSchema(wideTable);

        // idea: we have to group by cityName
        // 5 different unique values
        // aggregate them all in separate scans

        uint32_t selectionLength = 24;
        std::unique_ptr<char[]> selection(new char[selectionLength]);

        crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
        selectionWriter.write<uint64_t>(0x1u);

        selectionWriter.write<uint16_t>(context.valueTypeId);
        selectionWriter.write<uint16_t>(0x1u);
        selectionWriter.align(sizeof(uint64_t));
        selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::EQUAL));
        selectionWriter.write<uint8_t>(0x0u);
        selectionWriter.align(sizeof(uint32_t));
        selectionWriter.write<int32_t>(in.subscriber_value_type);

        // sort aggregation attributes
        id_t costSumAllIdx = in.window_length == 0 ?
                    context.costSumAllDay : context.costSumAllWeek;
        id_t durSumAllIdx = in.window_length == 0 ?
                    context.durSumAllDay : context.durSumAllWeek;
        id_t callsSumAllIdx = in.window_length == 0 ?
                    context.callsSumAllDay : context.callsSumAllWeek;
        std::map<id_t, std::tuple<FieldType, crossbow::string>> aggregationAttributes;
        aggregationAttributes[context.subscriberId] = std::make_tuple(
                FieldType::BIGINT, "subscriber_id");
        aggregationAttributes[costSumAllIdx] = std::make_tuple(
                FieldType::DOUBLE, "cost_sum_all");
        aggregationAttributes[durSumAllIdx] = std::make_tuple(
                FieldType::BIGINT, "dur_sum_all");
        aggregationAttributes[callsSumAllIdx] = std::make_tuple(
                FieldType::INT, "calls_sum_all");

        Schema resultSchema(schema.type());

        uint32_t aggregationLength = 2 * aggregationAttributes.size();
        std::unique_ptr<char[]> aggregation(new char[aggregationLength]);

        crossbow::buffer_writer aggregationWriter(aggregation.get(), aggregationLength);
        for (auto &attribute : aggregationAttributes) {
            aggregationWriter.write<uint16_t>(attribute.first);
            resultSchema.addField(std::get<0>(attribute.second),
                    std::get<1>(attribute.second), true);
        }

        Table resultTable(wideTable.value, std::move(resultSchema));

        auto &snapshot = tx.snapshot();
        auto &clientHandle = tx.getHandle();
        auto scanIterator = clientHandle.scan(resultTable, snapshot,
                    *context.scanMemoryMananger, ScanQueryType::AGGREGATION, selectionLength,
                    selection.get(), aggregationLength, aggregation.get());

        result.flat_rate = std::numeric_limits<double>().max();
        result.subscriber_id = 1;
        while (scanIterator->hasNext()) {
            const char* tuple;
            size_t tupleLength;
            std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
            int32_t callsSumAll;
            int64_t durSumAll;
            if ((callsSumAll = resultTable.field<int32_t>(
                        "calls_sum_all", tuple)) &&
                            (durSumAll = resultTable.field<int64_t>(
                                "dur_sum_all", tuple))) {
                double flatRate = resultTable.field<double>(
                        "cost_sum_all", tuple);
                flatRate /= durSumAll;
                if (flatRate < result.flat_rate) {
                    result.flat_rate = flatRate;
                    result.subscriber_id = resultTable.field<int64_t>(
                            "subscriber_sum_all", tuple);
                }
            }
        }

        result.success = true;
        tx.commit();

        if (scanIterator->error()) {
            result.error = crossbow::to_string(scanIterator->error().value());
            result.success = false;
            return result;
        }

    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

} // namespace aim

