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

