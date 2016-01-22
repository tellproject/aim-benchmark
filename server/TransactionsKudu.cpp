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
#include "TransactionsKudu.hpp"
#include "kudu.hpp"
#include <kudu/client/row_result.h>

#include <boost/unordered_map.hpp>

#include "Connection.hpp"

namespace aim {

using namespace kudu;
using namespace kudu::client;

using ScannerList = std::vector<std::unique_ptr<KuduScanner>>;

template<class T>
struct is_string {
    constexpr static bool value = false;
};

template<size_t S>
struct is_string<char[S]> {
    constexpr static bool value = true;
};

template<>
struct is_string<const char*> {
    constexpr static bool value = true;
};

template<>
struct is_string<std::string> {
    constexpr static bool value = true;
};

template<class T>
struct CreateValue {
    template<class U = T>
    typename std::enable_if<std::is_integral<U>::value, KuduValue*>::type
    create(U v) {
        return KuduValue::FromInt(v);
    }

    template<class U = T>
    typename std::enable_if<is_string<U>::value, KuduValue*>::type
    create(const U& v) {
        return KuduValue::CopyString(v);
    }
};


template<class... P>
struct PredicateAdder;

template<class Key, class Value, class... Tail>
struct PredicateAdder<Key, Value, Tail...> {
    PredicateAdder<Tail...> tail;
    CreateValue<Value> creator;

    void operator() (KuduTable& table, KuduScanner& scanner, const Key& key, const Value& value, const Tail&... rest) {
        assertOk(scanner.AddConjunctPredicate(table.NewComparisonPredicate(key, KuduPredicate::EQUAL, creator.template create<Value>(value))));
        tail(table, scanner, rest...);
    }
};

template<>
struct PredicateAdder<> {
    void operator() (KuduTable& table, KuduScanner& scanner) {
    }
};

template<class... Args>
void addPredicates(KuduTable& table, KuduScanner& scanner, const Args&... args) {
    PredicateAdder<Args...> adder;
    adder(table, scanner, args...);
}

template<class... Args>
KuduRowResult get(KuduTable& table, ScannerList& scanners, const Args&... args) {
    scanners.emplace_back(new KuduScanner(&table));
    auto& scanner = *scanners.back();
    addPredicates(table, scanner, args...);
    assertOk(scanner.Open());
    assert(scanner.HasMoreRows());
    std::vector<KuduRowResult> rows;
    assertOk(scanner.NextBatch(&rows));
    return rows[0];
}

void set(KuduWriteOperation& upd, const Slice& slice, int16_t v) {
    assertOk(upd.mutable_row()->SetInt16(slice, v));
}

void set(KuduWriteOperation& upd, const Slice& slice, int32_t v) {
    assertOk(upd.mutable_row()->SetInt32(slice, v));
}

void set(KuduWriteOperation& upd, const Slice& slice, int64_t v) {
    assertOk(upd.mutable_row()->SetInt64(slice, v));
}

void set(KuduWriteOperation& upd, const Slice& slice, std::nullptr_t) {
    assertOk(upd.mutable_row()->SetNull(slice));
}

void set(KuduWriteOperation& upd, const Slice& slice, const Slice& str) {
    assertOk(upd.mutable_row()->SetString(slice, str));
}

void Transactions::processEvent(Transaction& tx,
            Context &context, std::vector<Event> &events) {

    try {
        std::tr1::shared_ptr<KuduTable> wTable;
        session.client()->OpenTable("wt", &wTable);

        ScannerList scanners;
        std::vector<KuduRowResult> tupleFutures;
        tupleFutures.reserve(events.size());

        // get futures in reverse order
        for (auto iter = events.rbegin(); iter < events.rend(); ++iter) {
            tupleFutures.emplace_back(
                        get(*wTable, scanners, "subscriber_id", iter->caller_id));
        }

        auto eventIter = events.begin();
        // get the actual values in reverse reverse = actual order
        for (auto iter = tupleFutures.rbegin();
                    iter < tupleFutures.rend(); ++iter, ++eventIter) {

            auto& oldTuple = *iter;
            Timestamp ts =  oldTuple[context.timeStampId].value<Timestamp>();

            auto ins = table->NewInsert();
            auto newTuple = ins->mutable_row();

            for (auto &pair: context.tellIDToAIMSchemaEntry) {
                if (pair.second.filter(oldTuple))
                    pair.second.update(newTuple[pair.first], ts, *eventIter);
                else
                    pair.second.maintain(newTuple[pair.first], ts, *eventIter);
            }
            tx.update(wideTable, tell::db::key_t{eventIter->caller_id},
                      oldTuple, newTuple);
        }

        tx.commit();
    } catch (std::exception& ex) {
        LOG_ERROR("FATAL: Connection aborted for event, this must not happen, ex = %1%", ex.what());
        std::terminate();
    }
}


StockLevelResult Transactions::stockLevel(KuduSession& session, const StockLevelIn& in) {
    StockLevelResult result;
    result.low_stock = 0;
    try {
        std::tr1::shared_ptr<KuduTable> sTable;
        std::tr1::shared_ptr<KuduTable> olTable;
        std::tr1::shared_ptr<KuduTable> oTable;
        std::tr1::shared_ptr<KuduTable> dTable;
        session.client()->OpenTable("district", &dTable);
        session.client()->OpenTable("order", &oTable);
        session.client()->OpenTable("order-line", &olTable);
        session.client()->OpenTable("stock", &sTable);

        ScannerList scanners;
        // get District
        auto district = get(*dTable, scanners, "d_w_id", in.w_id, "d_id", in.d_id);
        int32_t d_next_o_id;
        assertOk(district.GetInt32("d_next_o_id", &d_next_o_id));

        scanners.emplace_back(new KuduScanner(olTable.get()));
        auto& olScanner = scanners.back();
        addPredicates(*olTable, *olScanner, "ol_w_id", in.w_id, "ol_d_id", in.d_id);
        assertOk(olScanner->AddConjunctPredicate(olTable->NewComparisonPredicate("ol_o_id", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(d_next_o_id - 20))));
        olScanner->Open();

        // count low_stock
        result.low_stock = 0;
        std::vector<KuduRowResult> rows;
        std::unique_ptr<KuduScanner> scanner;
        while (olScanner->HasMoreRows()) {
            olScanner->NextBatch(&rows);
            for (auto& row : rows) {
                int32_t ol_i_id;
                assertOk(row.GetInt32("ol_i_id", &ol_i_id));
                scanner.reset(new KuduScanner(sTable.get()));
                addPredicates(*sTable, *scanner, "s_w_id", in.w_id, "s_i_id", ol_i_id);
                scanner->Open();
                std::vector<KuduRowResult> stocks;
                while (scanner->HasMoreRows()) {
                    scanner->NextBatch(&stocks);
                    for (auto& stock : stocks) {
                        int32_t s_quantity;
                        assertOk(stock.GetInt32("s_quantity", &s_quantity));
                        if (s_quantity < in.threshold) {
                            ++result.low_stock;
                        }
                    }
                }
            }
        }
        result.success = true;
        return result;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }
    return result;
}

} // namespace aim
