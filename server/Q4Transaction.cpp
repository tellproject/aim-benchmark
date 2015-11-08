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

} // namespace aim

