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

            uint32_t selectionLength = 32;
            std::unique_ptr<char[]> selection(new char[selectionLength]);

            crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
            selectionWriter.write<uint32_t>(0x1u);
            selectionWriter.write<uint32_t>(0x1u);
            selectionWriter.write<uint32_t>(0x0u);
            selectionWriter.write<uint32_t>(0x0u);

            selectionWriter.write<uint16_t>(context.regionCountry);
            selectionWriter.write<uint16_t>(0x1u);
            selectionWriter.advance(4);
            selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::EQUAL));
            selectionWriter.write<uint8_t>(0x0u);
            selectionWriter.advance(2);
            selectionWriter.write<int32_t>(in.country_id);

            // sort aggregation attributes
            std::map<id_t, std::tuple<AggregationType, FieldType,
                    crossbow::string>> aggregationAttributes;
            aggregationAttributes[context.durMaxLocalWeek] = std::make_tuple(
                    AggregationType::MAX, FieldType::INT, "max_local_week");
            aggregationAttributes[context.durMaxLocalDay] = std::make_tuple(
                    AggregationType::MAX, FieldType::INT, "max_local_day");
            aggregationAttributes[context.durMaxDistantWeek] = std::make_tuple(
                    AggregationType::MAX, FieldType::INT, "max_distant_week");
            aggregationAttributes[context.durMaxDistantDay] = std::make_tuple(
                    AggregationType::MAX, FieldType::INT, "max_distant_day");

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
                result.max_local_week = resultTable.field<int32_t>("max_local_week", tuple);
                result.max_local_day = resultTable.field<int32_t>("max_local_day", tuple);
                result.max_distant_week = resultTable.field<int32_t>("max_distant_week", tuple);
                result.max_distant_day = resultTable.field<int32_t>("max_distant_day", tuple);
            }

            if (scanIterator->error()) {
                result.error = crossbow::to_string(scanIterator->error().value());
                result.success = false;
                return result;
            }
        }

        {   // find the corresponding IDs with 4 parallel scans

            uint32_t selectionLength = 32;
            std::unique_ptr<char[]> selection(new char[selectionLength]);

            crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
            selectionWriter.write<uint32_t>(0x1u);
            selectionWriter.write<uint32_t>(0x1u);
            selectionWriter.write<uint32_t>(0x0u);
            selectionWriter.write<uint32_t>(0x0u);

            auto columnPtr = selectionWriter.data();
            selectionWriter.write<uint16_t>(context.durMaxLocalWeek);   // we are going to vary this
            selectionWriter.write<uint16_t>(0x1u);
            selectionWriter.align(sizeof(uint64_t));
            selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::EQUAL));
            selectionWriter.write<uint8_t>(0x0u);
            selectionWriter.align(sizeof(uint32_t));
            auto valuePtr = selectionWriter.data();
            selectionWriter.write<int32_t>(result.max_local_week);      // we are going to vary this

            std::vector<std::pair<id_t, int32_t>> selectionValues;
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
                *(reinterpret_cast<int16_t*>(columnPtr)) = selectionValues[i].first;
                *(reinterpret_cast<int32_t*>(valuePtr)) = selectionValues[i].second;
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

} // namespace aim

