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

} // namespace aim

