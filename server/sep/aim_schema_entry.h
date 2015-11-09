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
#pragma once

#include <algorithm>
#include <cstring>
#include <limits>

#include <telldb/Field.hpp>

#include <common/Protocol.hpp>
#include "server/sep/value.h"
#include "server/sep/window.h"

using namespace aim;

/*
 * SchemaEntry: describes an attribute in the AM record. Each value has a Window
 * and a Value type. At construction time we define functions responsible for
 * initializing, updating and maintaining an AM attribute.
 *
 * _value    =  aggregation over a metric (call, duration, cost)
 *
 * _window   = tumbling, stepwise, continuous
 *
 * _size     = keeps the size of the entry
 *
 * _init_def = default initialization based on aggregation type (min, max,
 *             sum).
 *
 * _init     = initialization based on aggregation type (min, max, sum)
 *             and incoming event.
 *
 * _maintain = resets entry or keeps previous value.
 *
 * _update   = updates value based on aggregation type, window type, current event
 *             and previous subscriber record.
 *
 * _filter   = defines a filter, e.g. long calls, non local cost.
 */
class AIMSchemaEntry
{
public:
    typedef tell::db::Field (*InitDefFPtr)();
    typedef tell::db::Field& (*InitFPtr)(tell::db::Field&, const Event&);

    typedef tell::db::Field& (*UpdateFPtr)(tell::db::Field& , const AIMSchemaEntry&,
                                   Timestamp, const Event&);

    typedef tell::db::Field& (*MaintainFPtr)(tell::db::Field&, const AIMSchemaEntry&,
                                     Timestamp, const Event&);

    typedef bool (*FilterFPtr)(const Event&);

    AIMSchemaEntry(Value value, Window window, InitDefFPtr init_def, InitFPtr init,
                UpdateFPtr update, MaintainFPtr maintain, FilterType filter_type,
                FilterFPtr filter);
public:
    /*
     * The following functions are wrappers for the various function pointers.
     * All these functions take a pointer to char* corresponding to the full
     * record, compute the value that should be written, write the value to the
     * position the pointer indicates and forward the pointer. They also take a
     * char* pointer for writting the proper value to the compact record.
     */
    tell::db::Field initDef() const
    {
        return _init_def();
    }

    tell::db::Field &init(tell::db::Field& field, const Event& e) const
    {
        return _init(field, e);
    }

    tell::db::Field &update(tell::db::Field &field,
                    const AIMSchemaEntry& se,
                    Timestamp old_ts, const Event& e) const
    {
        return _update(field, se, old_ts, e);
    }

    tell::db::Field &maintain(tell::db::Field &field,
                    const AIMSchemaEntry& se,
                    Timestamp old_ts, const Event& e) const
    {
        return _maintain(field, se, old_ts, e);
    }

    bool filter(const Event& event) const
    {
        return _filter(event);
    }

public:
    AggrFun valAggrFun() const { return _value.aggrFun(); }
    Metric valMetric() const { return _value.metric(); }
    Timestamp winInitInfo() const { return _window.initInfo(); }
    Timestamp winDuration() const { return _window.duration(); }
    WindowLength winLength() const { return _window.length();}
    FilterType filterType() const { return _filter_type; }
    uint16_t size() const { return _size; }

    tell::store::FieldType type() const { return _value.type(); }
    const crossbow::string &name() const { return _value.name(); }

private:
    Value _value;
    Window _window;
    uint16_t _size;
    InitDefFPtr _init_def;
    InitFPtr _init;
    UpdateFPtr _update;
    MaintainFPtr _maintain;
    FilterType _filter_type;
    FilterFPtr _filter;
};

/*
 * We use the following structs as templates parameters to the functions that
 * initialize and update the attributes. This way we simulate the switch
 * between the different metrics. The default functions are used when the
 * filter function results to false and we want default initialization
 * (e.g. 0 for sum, max value for the min aggregation).
 */
struct CostExtractor
{
    typedef double type;
    static type extract(const Event &e) { return e.cost; }
    static type def() { return 0.0; }
};

struct CallExtractor
{
    typedef int32_t type;
    static type extract(const Event &e) { return 1; }
    static type def() { return 0; }
};

struct DurExtractor
{
    typedef int32_t type;
    static type extract(const Event &e) { return e.duration; }
    static type def() { return 0; }
};

/*
 * For the following functions:
 * f_tmp -> pointer to the full record
 * r_tmp -> pointer to the record we retrieved - previous record of this subscriber.
 * In all the initDef, init, update and maintain functions we generate both the
 * full and the compact record.
 */


/*
 * These functions are used for default initialization of an attribute value.
 */
template <typename Extractor>
tell::db::Field
initSumDef()
{
    return tell::db::Field(Extractor::def());
}

template <typename Extractor>
tell::db::Field
initMaxDef()
{
    return tell::db::Field(
                std::numeric_limits<typename Extractor::type>::min());
}

template <typename Extractor>
tell::db::Field
initMinDef()
{
    return tell::db::Field(
                std::numeric_limits<typename Extractor::type>::max());
}

/*
 * The functions below are used for normal initialization using event attribute
 * values. One function for doing this is actually enough
 */
template <typename Extractor>
tell::db::Field &initSumMinMax(tell::db::Field &field, const Event &e)
{
    return (field = tell::db::Field(Extractor::extract(e)));
}

/*
 * If the new event belongs to the same window with the previous one we simply
 * write the exact same value as before. Otherwise we reset the value.
 */
template <typename Extractor>
tell::db::Field&
maintain(tell::db::Field &field, const AIMSchemaEntry &se,
         Timestamp old_ts, const Event &e)
{
    Timestamp win_start = (old_ts - se.winInitInfo()) / se.winDuration();
    win_start = win_start * se.winDuration() + se.winInitInfo();

    if (e.timestamp <= win_start + se.winDuration()) { //belong to the same window
        //copy previous value the same value
        return field;
    }
    else {                                        //different windows->def init
        return (field = se.initDef());               //init function forwards field
    }
}

template <typename Extractor>
tell::db::Field&
updateSum(tell::db::Field &field, const AIMSchemaEntry &se,
          Timestamp old_ts, const Event &e)
{
    Timestamp win_start;
    auto t_val = Extractor::extract(e);     //take the new value from the event

    win_start = (old_ts - se.winInitInfo()) / se.winDuration();   //calculating closest Monday
    win_start = win_start * se.winDuration() + se.winInitInfo();  //when the window starts

    if (e.timestamp <= win_start + se.winDuration()) {
        return ((field) += tell::db::Field(t_val));
    }
    else {
        return (field = tell::db::Field(t_val));
    }
}

template <typename Extractor>
tell::db::Field&
updateMax(tell::db::Field &field, const AIMSchemaEntry &se,
          Timestamp old_ts, const Event &e)
{
    Timestamp win_start;
    auto t_val = Extractor::extract(e);     //take the new value from the event

    win_start = (old_ts - se.winInitInfo()) / se.winDuration();   //calculating closest Monday
    win_start = win_start * se.winDuration() + se.winInitInfo();  //when the window starts

    if (e.timestamp <= win_start + se.winDuration() && field >= tell::db::Field(t_val)) {
        return field;
    }
    return (field = tell::db::Field(t_val));
}

template <typename Extractor>
tell::db::Field&
updateMin(tell::db::Field &field, const AIMSchemaEntry &se,
          Timestamp old_ts, const Event &e)
{
    Timestamp win_start;
    auto t_val = Extractor::extract(e);     //take the new value from the event

    win_start = (old_ts - se.winInitInfo()) / se.winDuration();   //calculating closest Monday
    win_start = win_start * se.winDuration() + se.winInitInfo();  //when the window starts

    if (e.timestamp <= win_start + se.winDuration() && field <= tell::db::Field(t_val)) {
        return field;
    }
    return (field = tell::db::Field(t_val));
}

/*
 * Filters: an AM value is updated only if the respective filter evalutes to
 * true. In this is not the case, the value is maintained.
 */
inline bool noFilter(const Event &e) { return true; }

inline bool localFilter(const Event &e) { return !e.long_distance; }

inline bool nonlocalFilter(const Event &e) { return e.long_distance; }
