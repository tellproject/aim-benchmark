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
 * The template argument T is the type of the schema entry itself. It can either
 * be a primitive type (like int or double) or a specific database field type
 * (e.g. tell::db::Field)
 *
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
template <typename T>
class AIMSchemaEntry
{
public:
    typedef T (*InitDefFPtr)();
    typedef T& (*InitFPtr)(T&, const Event&);

    typedef T& (*UpdateFPtr)(T& , const AIMSchemaEntry&,
                                   Timestamp, const Event&);

    typedef T& (*MaintainFPtr)(T&, const AIMSchemaEntry&,
                                     Timestamp, const Event&);

    typedef bool (*FilterFPtr)(const Event&);

public:
    /*
     * The following functions are wrappers for the various function pointers.
     */
    T initDef() const
    {
        return _init_def();
    }

    T &init(T& field, const Event& e) const
    {
        return _init(field, e);
    }

    T &update(T &field,
                    Timestamp old_ts, const Event& e) const
    {
        return _update(field, *this, old_ts, e);
    }

    T &maintain(T &field,
                    Timestamp old_ts, const Event& e) const
    {
        return _maintain(field, *this, old_ts, e);
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
    using sum_type = double;
    static type extract(const Event &e) { return e.cost; }
    static type def() { return 0.0; }
};

struct CallExtractor
{
    typedef int32_t type;
    using sum_type = int32_t;
    static type extract(const Event &e) { return 1; }
    static type def() { return 0; }
};

struct DurExtractor
{
    typedef int32_t type;
    using sum_type = int64_t;
    static type extract(const Event &e) { return e.duration; }
    static type def() { return 0; }
};


/*
 * These functions are used for default initialization of an attribute value.
 */

template <typename Extractor>
typename Extractor::sum_type
initSumDef()
{
    return Extractor::sum_type(Extractor::def());
}

template <typename Extractor>
typename Extractor::type
initMaxDef()
{
    return std::numeric_limits<typename Extractor::type>::min();
}

template <typename Extractor>
typename Extractor::type
initMinDef()
{
    return std::numeric_limits<typename Extractor::type>::max();
}

/*
 * The functions below are used for normal initialization using event attribute
 * values. For min and max we can use the same function here
 */

template <typename Extractor>
typename Extractor::type &initMinMax(const Event &e)
{
    return Extractor::extract(e);
}

template <typename Extractor>
typename Extractor::sum_type &initSum(const Event &e)
{
    return Extractor::sum_type(Extractor::extract(e));
}

/*
 * The following functions contain specializations to wrap values
 * into something else of type T (e.g. into a field)
 */

template <typename Extractor, typename T>
T
initSumDef()
{
    return T(initSumDef<Extractor>());
}

template <typename Extractor, typename T>
T
initMaxDef()
{
    return T(initMaxDef<Extractor>());
}

template <typename Extractor, typename T>
T
initMinDef()
{
    return T(initMinDef<Extractor>());
}

template <typename Extractor, typename T>
T &initMinMax(T &field, const Event &e)
{
    return (field = T(Extractor::initMinMax));
}

template <typename Extractor, typename T>
T &initSum(T &field, const Event &e)
{
    return (field = T(Extractor::simpleInitSum(e)));
}

/*
 * If the new event belongs to the same window with the previous one we simply
 * write the exact same value as before. Otherwise we reset the value.
 */

template <typename T>
inline bool belongToSameWindow(const AIMSchemaEntry<T> &se,
        Timestamp old_ts, const Event &e) {
    Timestamp win_start = (old_ts - se.winInitInfo()) / se.winDuration();
    win_start = win_start * se.winDuration() + se.winInitInfo();

    return (e.timestamp <= win_start + se.winDuration());
}

template <typename Extractor, typename T>
T&
maintain(T &value, const AIMSchemaEntry<T> &se,
         Timestamp old_ts, const Event &e)
{
    if (belongToSameWindow(se, old_ts, e)) {
        //copy previous value the same value
        return value;
    }
    else {                                  //different windows->def init
        return (value = se.initDef());      //init function forwards field
    }
}

template <typename Extractor, typename T>
T&
updateSum(T &value, const AIMSchemaEntry<T> &se,
          Timestamp old_ts, const Event &e)
{
    using sum_type = typename Extractor::sum_type;
    auto t_val = sum_type(Extractor::extract(e));     //take the new value from the event


    if (belongToSameWindow(se, old_ts, e)) {
        return ((value) += T(t_val));
    }
    else {
        return (value = T(t_val));
    }
}

template <typename Extractor, typename T>
T&
updateMax(T &value, const AIMSchemaEntry<T> &se,
          Timestamp old_ts, const Event &e)
{
    auto t_val = Extractor::extract(e);     //take the new value from the event

    if (belongToSameWindow(se, old_ts, e) && value >= T(t_val)) {
        return value;
    }
    return (value = T(t_val));
}

template <typename Extractor, typename T>
T&
updateMin(T &value, const AIMSchemaEntry<T> &se,
          Timestamp old_ts, const Event &e)
{
    auto t_val = Extractor::extract(e);     //take the new value from the event

    if (belongToSameWindow(se, old_ts, e) && value <= T(t_val)) {
        return value;
    }
    return (value = T(t_val));
}

/*
 * Filters: an AM value is updated only if the respective filter evalutes to
 * true. In this is not the case, the value is maintained.
 */
inline bool noFilter(const Event &e) { return true; }

inline bool localFilter(const Event &e) { return !e.long_distance; }

inline bool nonlocalFilter(const Event &e) { return e.long_distance; }
