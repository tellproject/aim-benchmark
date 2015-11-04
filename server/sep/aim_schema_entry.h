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
    typedef uint16_t (*InitDefFPtr)(char**);
    typedef uint16_t (*InitFPtr)(char**, const Event&);

    typedef uint16_t (*UpdateFPtr)(char**, const char**, const AIMSchemaEntry&,
                                   Timestamp, const Event&);

    typedef uint16_t (*MaintainFPtr)(char**, const char**, const AIMSchemaEntry&,
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
    uint16_t initDef(char **tmp) const
    {
        return _init_def(tmp);
    }

    uint16_t init(char **tmp, const Event& e) const
    {
        return _init(tmp, e);
    }

    uint16_t update(char **tmp, const char **r_tmp, const AIMSchemaEntry& se,
                    Timestamp old_ts, const Event& e) const
    {
        return _update(tmp, r_tmp, se, old_ts, e);
    }

    uint16_t maintain(char** tmp, const char** r_tmp, const AIMSchemaEntry& se,
                      Timestamp old_ts, const Event& e) const
    {
        return _maintain(tmp, r_tmp, se, old_ts, e);
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

    //lb: added for generic benchmarking framework
    DataType type() const { return _value.type(); }

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
    typedef uint type;
    static type extract(const Event &e) { return 1; }
    static type def() { return 0; }
};

struct DurExtractor
{
    typedef uint type;
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
uint16_t
initSumDef(char **tmp)
{
    auto t_val = Extractor::def();
    memcpy(*tmp, &t_val, sizeof(t_val));
    *tmp += sizeof(t_val);
    return (uint16_t)sizeof(t_val);
}

template <typename Extractor>
uint16_t
initMaxDef(char **tmp)
{
    auto t_val = std::numeric_limits<typename Extractor::type>::min();
    memcpy(*tmp, &t_val, sizeof(t_val));
    *tmp += sizeof(t_val);
    return (uint16_t)sizeof(t_val);
}

template <typename Extractor>
uint16_t
initMinDef(char **tmp)
{
    auto t_val = std::numeric_limits<typename Extractor::type>::max();
    memcpy(*tmp, &t_val, sizeof(t_val));
    *tmp += sizeof(t_val);
    return (uint16_t)sizeof(t_val);
}

/*
 * The functions below are used for normal initialization using event attribute
 * values.
 */
template <typename Extractor>
uint16_t initSum(char **tmp, const Event &e)
{
    auto t_val = Extractor::extract(e);
    memcpy(*tmp, &t_val, sizeof(t_val));
    *tmp += sizeof(t_val);
    return (uint16_t)sizeof(t_val);
}

template <typename Extractor>
uint16_t
initMax(char **tmp, const Event &e)
{
    auto t_val = Extractor::extract(e);
    memcpy(*tmp, &t_val, sizeof(t_val));
    *tmp += sizeof(t_val);
    return (uint16_t)sizeof(t_val);
}

template <typename Extractor>
uint16_t
initMin(char **tmp, const Event &e)
{
    auto t_val = Extractor::extract(e);
    memcpy(*tmp, &t_val, sizeof(t_val));
    *tmp += sizeof(t_val);
    return (uint16_t)sizeof(t_val);
}

/*
 * If the new event belongs to the same window with the previous one we simply
 * write the exact same value as before. Otherwise we reset the value.
 */
template <typename Extractor>
uint16_t
maintain(char **tmp, const char **r_tmp, const AIMSchemaEntry &se,
         Timestamp old_ts, const Event &e)
{
    Timestamp win_start = (old_ts - se.winInitInfo()) / se.winDuration();
    win_start = win_start * se.winDuration() + se.winInitInfo();

    if (e.timestamp <= win_start + se.winDuration()) { //belong to the same window
        memcpy(*tmp, *r_tmp, se.size());                 //copy previous value
        *r_tmp += se.size();                             //the same value
        *tmp += se.size();
        return se.size();
    }
    else {                                        //different windows->def init
        uint16_t written = se.initDef(tmp);       //init function forwards tmp
        *r_tmp += written;
        return written;
    }
}

template <typename Extractor>
uint16_t
updateSum(char **tmp, const char **r_tmp, const AIMSchemaEntry &se,
          Timestamp old_ts, const Event &e)
{
    Timestamp win_start;
    auto t_val = Extractor::extract(e);     //take the new value from the event
    typename Extractor::type t_sum;

    memcpy(&t_sum, *r_tmp, sizeof(t_sum));
    *r_tmp += sizeof(t_sum);

    win_start = (old_ts - se.winInitInfo()) / se.winDuration();   //calculating closest Monday
    win_start = win_start * se.winDuration() + se.winInitInfo();  //when the window starts

    if (e.timestamp <= win_start + se.winDuration()) {
        t_sum += t_val;
    }
    else {
        t_sum = t_val;
    }
    memcpy(*tmp, &t_sum, sizeof(t_sum));
    *tmp += sizeof(t_sum);
    return (uint16_t)sizeof(t_sum);
}

template <typename Extractor>
uint16_t
updateMax(char **tmp, const char **r_tmp, const AIMSchemaEntry &se,
          Timestamp old_ts, const Event &e)
{
    Timestamp win_start;
    auto t_val = Extractor::extract(e);
    typename Extractor::type t_max;

    memcpy(&t_max, *r_tmp, sizeof(t_max));
    *r_tmp += sizeof(t_max);

    win_start = (old_ts - se.winInitInfo()) / se.winDuration();   //calculating closest Monday
    win_start = win_start * se.winDuration() + se.winInitInfo();  //when the window starts

    if (e.timestamp <= win_start + se.winDuration()) {
        t_max = std::max(t_max, t_val);
    }
    else {
        t_max = t_val;
    }
    memcpy(*tmp, &t_max, sizeof(t_max));
    *tmp += sizeof(t_max);
    return (uint16_t)sizeof(t_max);
}

template <typename Extractor>
uint16_t
updateMin(char **tmp, const char **r_tmp, const AIMSchemaEntry &se,
          Timestamp old_ts, const Event &e)
{
    Timestamp win_start;
    auto t_val = Extractor::extract(e);
    typename Extractor::type t_min;

    memcpy(&t_min, *r_tmp, sizeof(t_min));
    *r_tmp += sizeof(t_min);

    win_start = (old_ts - se.winInitInfo()) / se.winDuration();  //calculating closest Monday
    win_start = win_start * se.winDuration() + se.winInitInfo(); //when the window starts

    if (e.timestamp <= win_start + se.winDuration()) {
        t_min = std::min(t_min, t_val);
    }
    else {
        t_min = t_val;
    }
    memcpy(*tmp, &t_min, sizeof(t_min));
    *tmp += sizeof(t_min);
    return (uint16_t)sizeof(t_min);
}

/*
 * Filters: an AM value is updated only if the respective filter evalutes to
 * true. In this is not the case, the value is maintained.
 */
inline bool noFilter(const Event &e) { return true; }

inline bool localFilter(const Event &e) { return !e.long_distance; }

inline bool nonlocalFilter(const Event &e) { return e.long_distance; }
