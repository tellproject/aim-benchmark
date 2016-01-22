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

#include <functional>

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

void set(KuduWriteOperation& upd, const Slice& slice, double v) {
    assertOk(upd.mutable_row()->SetDouble(slice, v));
}

void set(KuduWriteOperation& upd, const Slice& slice, std::nullptr_t) {
    assertOk(upd.mutable_row()->SetNull(slice));
}

void set(KuduWriteOperation& upd, const Slice& slice, const Slice& str) {
    assertOk(upd.mutable_row()->SetString(slice, str));
}

void getField(KuduRowResult &row, const std::string &columnName, int32_t &result) {
    assertOk(row.GetInt32(columnName, &result));
}

void getField(KuduRowResult &row, const std::string &columnName, int64_t &result) {
    assertOk(row.GetInt64(columnName, &result));
}

void getField(KuduRowResult &row, const std::string &columnName, double &result) {
    assertOk(row.GetDouble(columnName, &result));
}

using namespace aim;

template <typename T, typename Fun>
void updateTuple (Fun fun, const std::string &columName,
        KuduRowResult &oldTuple, KuduWriteOperation &upd, Event &event, Timestamp ts) {
    T value;
    getField(oldTuple, columName, value);
    tell::db::Field field (value);
    auto f = fun(field, ts, event);
    set(upd, columName, f.template value<T>());
}

void Transactions::processEvent(kudu::client::KuduSession& session,
            Context &context, std::vector<Event> &events) {

    try {
        std::tr1::shared_ptr<KuduTable> wTable;
        session.client()->OpenTable("wt", &wTable);

        ScannerList scanners;
        std::vector<KuduRowResult> oldTuples;
        oldTuples.reserve(events.size());

        // get futures in reverse order
        for (auto iter = events.rbegin(); iter < events.rend(); ++iter) {
            oldTuples.emplace_back(
                        get(*wTable, scanners, "subscriber_id", iter->caller_id));
        }

        auto eventIter = events.begin();
        // get the actual values in reverse reverse = actual order
        for (auto iter = oldTuples.rbegin();
                    iter < oldTuples.rend(); ++iter, ++eventIter) {

            auto& oldTuple = *iter;
            Timestamp ts;
            assertOk(oldTuple.GetInt64("last_updated", &ts));

            std::unique_ptr<KuduWriteOperation> upd(wTable->NewUpdate());

            for (auto &pair: context.tellIDToAIMSchemaEntry) {
                auto &schemaEntry = pair.second;
                std::function<tell::db::Field(tell::db::Field& ,Timestamp, const Event&)> fun;

                using namespace std::placeholders;
                if (schemaEntry.filter(*eventIter))
                    fun = std::bind(&AIMSchemaEntry::update, schemaEntry, _1, _2, _3);
                else
                    fun =std::bind(&AIMSchemaEntry::maintain, schemaEntry, _1, _2, _3);
                std::string fieldName (schemaEntry.name().c_str(), schemaEntry.name().size());
                switch (schemaEntry.type()) {
                case tell::store::FieldType::INT:
                    updateTuple<int32_t>(fun, fieldName, oldTuple, *upd, *eventIter, ts);
                    break;
                case tell::store::FieldType::BIGINT:
                    updateTuple<int64_t>(fun, fieldName, oldTuple, *upd, *eventIter, ts);
                    break;
                case tell::store::FieldType::DOUBLE:
                    updateTuple<double>(fun, fieldName, oldTuple, *upd, *eventIter, ts);
                    break;
                default:
                    //
                    assert(false);
                }
            }
            assertOk(session.Apply(upd.release()));
        }
        assertOk(session.Flush());
    } catch (std::exception& ex) {
        LOG_ERROR("FATAL: Connection aborted for event, this must not happen, ex = %1%", ex.what());
        std::terminate();
    }

    //TODO: continue with other transactions here...
}
