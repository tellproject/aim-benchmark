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
#include "event.h"

#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>

void
Event::print() const
{
    std::stringstream s;
    s << "call_id: " << call_id << std::endl;
    s << "callee: " << _callee_id << std::endl;
    s << "caller: " << _caller_id << std::endl;
    s << "time: " << _timestamp << std::endl;
    s << "dur: " << _duration << std::endl;
    s << "cost: " << _cost << std::endl;
    s << "long_dist: " << _long_distance << std::endl;
    s << "place_caller: " << _caller_place << std::endl;
    s << "place_calee: " << _callee_place << std::endl;
    std::cout << s.str() << std::endl;
}

inline ulong
Event::callId() const
{
    return call_id;
}

inline ulong
Event::callerId() const
{
    return _caller_id;
}

inline ulong
Event::calleeId() const
{
    return _callee_id;
}

inline double
Event::cost() const
{
    return _cost;
}

inline uint
Event::duration() const
{
    return _duration;
}

inline bool
Event::longDistance() const
{
    return _long_distance;
}

inline ulong
Event::placeCaller() const
{
    return _caller_place;
}

inline ulong
Event::placeCallee() const
{
    return _callee_place;
}

inline Timestamp
Event::timestamp() const
{
    return _timestamp;
}

inline void
Event::setCallId(ulong call_id)
{
    call_id = call_id;
}

inline void
Event::setCallerId(ulong caller_id)
{
    _caller_id = caller_id;
}

inline void
Event::setCalleeId(ulong callee_id)
{
    _callee_id = callee_id;
}

inline void
Event::setCost(double cost)
{
    _cost = cost;
}

inline void
Event::setDuration(uint duration)
{
    _duration = duration;
}

inline void
Event::setLongDistance(bool long_distance)
{
    _long_distance = long_distance;
}

inline void
Event::setPlaceCaller(ulong caller_place)
{
    _caller_place = caller_place;
}

inline void
Event::setPlaceCallee(ulong callee_place)
{
    _callee_place = callee_place;
}

inline void
Event::setTimestamp(Timestamp timestamp)
{
    _timestamp = timestamp;
}
