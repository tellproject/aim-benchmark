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
#include <vector>

#include <common/dimension-tables-unique-values.h>
#include "Connection.hpp"

namespace aim {

using namespace tell::db;

void Transactions::processEvent(Transaction& tx,
            Context &context, std::vector<Event> &events) {

    try {
        auto wFuture = tx.openTable("wt");
        auto wideTable = wFuture.get();

        std::vector<Future<Tuple>> tupleFutures;
        tupleFutures.reserve(events.size());

        // get futures in revers order
        for (auto iter = events.rbegin(); iter < events.rend(); ++iter) {
            tupleFutures.emplace_back(
                        tx.get(wideTable, tell::db::key_t{iter->caller_id}));
        }

        auto eventIter = events.begin();
        // get the actual values in reverse reverse = actual order
        for (auto iter = tupleFutures.rbegin();
                    iter < tupleFutures.rend(); ++iter, ++eventIter) {
            Tuple oldTuple = iter->get();
            Timestamp ts =  oldTuple[context.timeStampId].value<Timestamp>();
            Tuple newTuple (oldTuple);
            for (auto &pair: context.tellIDToAIMSchemaEntry) {
                pair.second.update(&newTuple[pair.first], pair.second, ts, *eventIter);
            }
            tx.update(wideTable, tell::db::key_t{eventIter->caller_id},
                      oldTuple, newTuple);
        }

        tx.commit();
    } catch (std::exception& ex) {
    }
}

} // namespace aim

