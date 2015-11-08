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

#include <map>

#include <common/dimension-tables-unique-values.h>
#include "Connection.hpp"

namespace aim {

using namespace tell::db;
using namespace tell::store;

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
                int32_t callsSumAllWeek  = projectionResultTable.field<int32_t>(
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

} // namespace aim

