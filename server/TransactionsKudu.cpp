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
#include <kudu/util/status.h>
#include <kudu/client/client.h>
#include <kudu/client/row_result.h>

#include <common/dimension-tables-unique-values.h>

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

template<class Key, class Value, class CompOp, class... Tail>
struct PredicateAdder<Key, Value, CompOp, Tail...> {
    PredicateAdder<Tail...> tail;
    CreateValue<Value> creator;

    void operator() (KuduTable& table, KuduScanner& scanner, const Key& key,
            const Value& value, const CompOp& predicate, const Tail&... rest) {
        assertOk(scanner.AddConjunctPredicate(table.NewComparisonPredicate(key, predicate, creator.template create<Value>(value))));
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

// opens a scan an puts it in the scanner list, use an empty projection-vector for getting everything
template<class... Args>
KuduScanner &openScan(KuduTable& table, ScannerList& scanners, const std::vector<std::string> &projectionColumns, const Args&... args) {
    scanners.emplace_back(new KuduScanner(&table));
    auto& scanner = *scanners.back();
    addPredicates(table, scanner, args...);
    if (projectionColumns.size() > 0) {
        scanner.SetProjectedColumnNames(projectionColumns);
    }
    assertOk(scanner.Open());
    return scanner;
}

// simple get request (retrieves one tuple)
template<class... Args>
KuduRowResult get(KuduTable& table, ScannerList& scanners, const Args&... args) {
    std::vector<std::string> emptyVec;
    auto& scanner = openScan(table, scanners, emptyVec, args...);
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

void getField(KuduRowResult &row, const std::string &columnName, int16_t &result) {
    assertOk(row.GetInt16(columnName, &result));
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

void getField(KuduRowResult &row,  int columnIdx, int16_t &result) {
    assertOk(row.GetInt16(columnIdx, &result));
}

void getField(KuduRowResult &row,  int columnIdx, int32_t &result) {
    assertOk(row.GetInt32(columnIdx, &result));
}

void getField(KuduRowResult &row,  int columnIdx, int64_t &result) {
    assertOk(row.GetInt64(columnIdx, &result));
}

void getField(KuduRowResult &row,  int columnIdx, double &result) {
    assertOk(row.GetDouble(columnIdx, &result));
}

namespace aim {

template <typename T, typename Fun>
void updateTuple (Fun fun, const std::string &columnName,
        KuduRowResult &oldTuple, KuduWriteOperation &upd, Event &event, Timestamp ts) {
    T value;
    getField(oldTuple, columnName, value);
    tell::db::Field field (value);
    auto f = fun(field, ts, event);
    set(upd, columnName, f.template value<T>());
}

void Transactions::processEvent(kudu::client::KuduSession& session, std::vector<Event> &events) {

    try {
        std::tr1::shared_ptr<KuduTable> wTable;
        assertOk(session.client()->OpenTable("wt", &wTable));

        ScannerList scanners;
        std::vector<KuduRowResult> oldTuples;
        oldTuples.reserve(events.size());

        // get futures in reverse order
        for (auto iter = events.rbegin(); iter < events.rend(); ++iter) {
            oldTuples.emplace_back(
                    get(*wTable, scanners, subscriberId, iter->caller_id, KuduPredicate::EQUAL));
        }

        auto eventIter = events.begin();
        // get the actual values in reverse reverse = actual order
        for (auto iter = oldTuples.rbegin();
                    iter < oldTuples.rend(); ++iter, ++eventIter) {

            auto& oldTuple = *iter;
            Timestamp ts;
            assertOk(oldTuple.GetInt64(timeStamp, &ts));

            std::unique_ptr<KuduWriteOperation> upd(wTable->NewUpdate());

            // maintain primary key
            set(*upd, subscriberId, int64_t(eventIter->caller_id));

            // update time stamp
            set(*upd, timeStamp, now());

            // update all AM attributes
            for (uint i = 0; i < mAimSchema.numOfEntries(); ++i) {
                auto &schemaEntry = mAimSchema[i];
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
}

Q1Out Transactions::q1Transaction(KuduSession &session, const Q1In &in)
{
    Q1Out result;

    try {
        std::tr1::shared_ptr<KuduTable> wTable;
        assertOk(session.client()->OpenTable("wt", &wTable));

        std::vector<std::string> projection;
        int durSumAllWeekIdx = projection.size();
        projection.push_back(durSumAllWeek);

        ScannerList scanners;
        std::vector<KuduRowResult> resultBatch;
        KuduScanner &scanner = openScan(*wTable, scanners, projection, callsSumLocalWeek, in.alpha+1, KuduPredicate::GREATER_EQUAL);
        int64_t sum = 0, cnt = 0, val;
        while (scanner.HasMoreRows()) {
            assertOk(scanner.NextBatch(&resultBatch));
            for (KuduRowResult row : resultBatch) {
                getField(row, durSumAllWeekIdx, val);
                sum += val;
                cnt++;
            }
        }

        result.avg = sum;
        if (cnt != 0)   // don-t divide by zero, report 0!
            result.avg /= cnt;
        result.success = true;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

Q2Out Transactions::q2Transaction(KuduSession &session, const Q2In &in)
{
    Q2Out result;

    try {
        std::tr1::shared_ptr<KuduTable> wTable;
        assertOk(session.client()->OpenTable("wt", &wTable));

        std::vector<std::string> projection;
        int costMaxAllWeekIdx = projection.size();
        projection.push_back(costMaxAllWeek);

        ScannerList scanners;
        std::vector<KuduRowResult> resultBatch;
        KuduScanner &scanner = openScan(*wTable, scanners, projection, callsSumAllWeek, in.alpha+1, KuduPredicate::GREATER_EQUAL);
        double cost;
        result.max = std::numeric_limits<double>::min();
        while (scanner.HasMoreRows()) {
            assertOk(scanner.NextBatch(&resultBatch));
            for (KuduRowResult row : resultBatch) {
                getField(row, costMaxAllWeekIdx, cost);
                result.max = std::max(result.max, cost);
            }
        }
        result.success = true;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

Q3Out Transactions::q3Transaction(KuduSession &session)
{
    Q3Out result;

    try {
        std::tr1::shared_ptr<KuduTable> wTable;
        assertOk(session.client()->OpenTable("wt", &wTable));

        std::vector<std::string> projection;
        int callsSumAllWeekIdx = projection.size();
        projection.push_back(callsSumAllWeek);
        int durSumAllWeekIdx = projection.size();
        projection.push_back(durSumAllWeek);
        int costSumAllWeekIdx = projection.size();
        projection.push_back(costSumAllWeek);

        ScannerList scanners;
        std::vector<KuduRowResult> resultBatch;
        KuduScanner &scanner = openScan(*wTable, scanners, projection);

        // callsSumAllWeek -> durSumAllWeek.sum, costSumAllWeek.sum
        boost::unordered_map<int32_t, std::pair<int64_t, double>> map;

        int32_t calls;
        int64_t dur;
        double cost;
        while (scanner.HasMoreRows()) {
            assertOk(scanner.NextBatch(&resultBatch));
            for (KuduRowResult row : resultBatch) {
                getField(row, callsSumAllWeekIdx, calls);
                getField(row, durSumAllWeekIdx, dur);
                getField(row, costSumAllWeekIdx, cost);

                auto it = map.find(calls);
                if (it == map.end())
                    it = map.emplace(std::make_pair(calls, std::make_pair(0, 0.0))).first;
                (it->second.first) += dur;
                (it->second.second) += cost;
            }
        }
        result.results.reserve(map.size());
        for (auto &entry: map) {
            Q3Out::Q3Tuple q3Tuple;
            q3Tuple.number_of_calls_this_week = entry.first;
            q3Tuple.cost_ratio = entry.second.second
                    / entry.second.first;
            result.results.push_back(std::move(q3Tuple));
        }
        result.success = true;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

Q4Out Transactions::q4Transaction(KuduSession &session, const Q4In &in)
{
    Q4Out result;

    try {
        std::tr1::shared_ptr<KuduTable> wTable;
        assertOk(session.client()->OpenTable("wt", &wTable));

        std::vector<std::string> projection;
        int regionCityIdx = projection.size();
        projection.push_back(regionCity);
        int callsSumLocalWeekIdx = projection.size();
        projection.push_back(callsSumLocalWeek);
        int durSumLocalWeekIdx = projection.size();
        projection.push_back(durSumLocalWeek);

        ScannerList scanners;
        std::vector<KuduRowResult> resultBatch;
        KuduScanner &scanner = openScan(*wTable, scanners, projection,
                callsSumLocalWeek, in.alpha+1, KuduPredicate::GREATER_EQUAL,
                durSumLocalWeek, in.beta+1, KuduPredicate::GREATER_EQUAL);

        // city-id -> (callsSumLocalWeek.cnt, callsSumLocalWeek.sum, durSumLocalWeek.sum)
        boost::unordered_map<int16_t, std::tuple<int32_t, int32_t, int64_t>> map;

        int16_t city_id;
        int32_t calls;
        int64_t dur;
        while (scanner.HasMoreRows()) {
            assertOk(scanner.NextBatch(&resultBatch));
            for (KuduRowResult row : resultBatch) {
                getField(row, regionCityIdx, city_id);
                getField(row, callsSumLocalWeekIdx, calls);
                getField(row, durSumLocalWeekIdx, dur);

                auto it = map.find(city_id);
                if (it == map.end())
                    it = map.emplace(std::make_pair(city_id, std::make_tuple(0, 0, 0))).first;
                (std::get<0>(it->second)) ++;
                (std::get<1>(it->second)) += calls;
                (std::get<2>(it->second)) += dur;
            }
        }
        result.results.reserve(map.size());
        for (auto &entry: map) {
            Q4Out::Q4Tuple q4Tuple;
            q4Tuple.city_name = region_unique_city[entry.first];
            q4Tuple.avg_num_local_calls_week =
                    static_cast<double>(std::get<1>(entry.second))
                            / std::get<0>(entry.second);
            q4Tuple.sum_duration_local_calls_week = std::get<2>(entry.second);
            result.results.push_back(std::move(q4Tuple));
        }
        result.success = true;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

Q5Out Transactions::q5Transaction(KuduSession &session, const Q5In &in)
{
    Q5Out result;

    try {
        std::tr1::shared_ptr<KuduTable> wTable;
        assertOk(session.client()->OpenTable("wt", &wTable));

        std::vector<std::string> projection;
        int regionRegionIdx = projection.size();
        projection.push_back(regionRegion);
        int costSumLocalWeekIdx = projection.size();
        projection.push_back(costSumLocalWeek);
        int costSumDistantWeekIdx = projection.size();
        projection.push_back(costSumDistantWeek);

        ScannerList scanners;
        std::vector<KuduRowResult> resultBatch;
        KuduScanner &scanner = openScan(*wTable, scanners, projection,
                subscriptionTypeId, in.sub_type, KuduPredicate::EQUAL,
                categoryId, in.sub_category, KuduPredicate::EQUAL);

        // region-id -> (costSumLocalWeek.sum, costSumDinstantWeek.sum)
        boost::unordered_map<int16_t, std::pair<int64_t, int64_t>> map;

        int16_t region_id;
        double local_cost, dist_cost;
        while (scanner.HasMoreRows()) {
            assertOk(scanner.NextBatch(&resultBatch));
            for (KuduRowResult row : resultBatch) {
                getField(row, regionRegionIdx, region_id);
                getField(row, costSumLocalWeekIdx, local_cost);
                getField(row, costSumDistantWeekIdx, dist_cost);

                auto it = map.find(region_id);
                if (it == map.end())
                    it = map.emplace(std::make_pair(region_id, std::make_pair(0.0, 0.0))).first;
                (it->second.first) += local_cost;
                (it->second.second) += dist_cost;
            }
        }
        result.results.reserve(map.size());
        for (auto &entry: map) {
            Q5Out::Q5Tuple q5Tuple;
            q5Tuple.region_name = region_unique_region[entry.first];
            q5Tuple.sum_cost_local_calls_week = entry.second.first;
            q5Tuple.sum_cost_longdistance_calls_week = entry.second.second;
            result.results.push_back(std::move(q5Tuple));
        }
        result.success = true;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

Q6Out Transactions::q6Transaction(KuduSession &session, const Q6In &in)
{
    Q6Out result;
    result.max_local_week = result.max_local_day = result.max_distant_week =
            result.max_distant_day = std::numeric_limits<int32_t>::min();

    try {
        std::tr1::shared_ptr<KuduTable> wTable;
        assertOk(session.client()->OpenTable("wt", &wTable));

        std::vector<std::string> projection;
        int subscriberIdIdx = projection.size();
        projection.push_back(subscriberId);
        int durMaxLocalWeekIdx = projection.size();
        projection.push_back(durMaxLocalWeek);
        int durMaxLocalDayIdx = projection.size();
        projection.push_back(durMaxLocalDay);
        int durMaxDistantWeekIdx = projection.size();
        projection.push_back(durMaxDistantWeek);
        int durMaxDistantDayIdx = projection.size();
        projection.push_back(durMaxDistantDay);

        ScannerList scanners;
        std::vector<KuduRowResult> resultBatch;
        KuduScanner &scanner = openScan(*wTable, scanners, projection,
                regionCountry, in.country_id, KuduPredicate::EQUAL);

        int32_t local_week, local_day, distant_week, distant_day;
        int64_t subscriber;
        while (scanner.HasMoreRows()) {
            assertOk(scanner.NextBatch(&resultBatch));
            for (KuduRowResult row : resultBatch) {
                getField(row, subscriberIdIdx, subscriber);
                getField(row, durMaxLocalWeekIdx, local_week);
                getField(row, durMaxLocalDayIdx, local_day);
                getField(row, durMaxDistantWeekIdx, distant_week);
                getField(row, durMaxDistantDayIdx, distant_day);

                if (local_week > result.max_local_week) {
                    result.max_local_week = local_week;
                    result.max_local_week_id = subscriber;
                }
                if (local_day > result.max_local_day) {
                    result.max_local_day = local_day;
                    result.max_local_day_id = subscriber;
                }
                if (distant_week > result.max_distant_week) {
                    result.max_distant_week = distant_week;
                    result.max_distant_week_id = subscriber;
                }
                if (distant_day > result.max_distant_day) {
                    result.max_distant_day = distant_day;
                    result.max_distant_day_id = subscriber;
                }
            }
        }
        result.success = true;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

Q7Out Transactions::q7Transaction(KuduSession &session, const Q7In &in)
{
    Q7Out result;
    result.flat_rate = std::numeric_limits<double>::max();

    std::string &costSum = in.window_length ? costSumAllWeek : costSumAllDay;
    std::string &durSum = in.window_length ? durSumAllWeek : durSumAllDay;
    std::string &callsSum = in.window_length ? callsSumAllWeek : callsSumAllDay;

    try {
        std::tr1::shared_ptr<KuduTable> wTable;
        assertOk(session.client()->OpenTable("wt", &wTable));

        std::vector<std::string> projection;
        int subscriberIdx = projection.size();
        projection.push_back(subscriberId);
        int costSumIdx = projection.size();
        projection.push_back(costSum);
        int durSumIdx = projection.size();
        projection.push_back(durSum);
        int callsSumIdx = projection.size();
        projection.push_back(callsSum);

        ScannerList scanners;
        std::vector<KuduRowResult> resultBatch;
        KuduScanner &scanner = openScan(*wTable, scanners, projection,
                valueTypeId, in.subscriber_value_type, KuduPredicate::EQUAL);

        int32_t calls;
        int64_t subscriber, dur;
        double cost;
        while (scanner.HasMoreRows()) {
            assertOk(scanner.NextBatch(&resultBatch));
            for (KuduRowResult row : resultBatch) {
                getField(row, subscriberIdx, subscriber);
                getField(row, costSumIdx, cost);
                getField(row, durSumIdx, dur);
                getField(row, callsSumIdx, calls);

                if (calls && dur) {
                    auto flatRate = cost;
                    flatRate /= dur;
                    if (flatRate < result.flat_rate) {
                        result.flat_rate = flatRate;
                        result.subscriber_id = subscriber;
                    }
                }
            }
        }
        result.success = true;
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    return result;
}

} // namespace aim
