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

        uint32_t selectionLength = 32;
        std::unique_ptr<char[]> selection(new char[selectionLength]);

        crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
        selectionWriter.write<uint32_t>(0x1u);
        selectionWriter.write<uint16_t>(0x1u);
        selectionWriter.write<uint16_t>(0x0u);
        selectionWriter.write<uint32_t>(0x0u);
        selectionWriter.write<uint32_t>(0x0u);

        selectionWriter.write<uint16_t>(context.regionRegion);
        selectionWriter.write<uint16_t>(0x1u);
        selectionWriter.advance(4);
        selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::EQUAL));
        selectionWriter.write<uint8_t>(0x0u);
        selectionWriter.advance(2);
        auto valuePtr = selectionWriter.data();
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
            *(reinterpret_cast<int32_t*>(valuePtr)) = i;
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

} // namespace aim

