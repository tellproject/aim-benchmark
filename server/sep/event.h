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

#include <cstdint>
#include <iostream>

#include "server/sep/utils.h"

/*
 * Struct Event: models an event in the system. It contains a number of fields
 * defined in the SEP benchmark.
 */
struct Event
{

public:
    ulong _call_id;
    ulong _caller_id;
    ulong _callee_id;
    double _cost;
    uint _duration;
    bool _long_distance;
    ulong _caller_place;
    ulong _callee_place;
    Timestamp _timestamp;

public:
    /*
     * Getters
     */
    ulong callId() const;
    ulong callerId() const;
    ulong calleeId() const;
    double cost() const;
    uint duration() const;
    bool longDistance() const;
    ulong placeCaller() const;
    ulong placeCallee() const;
    Timestamp timestamp() const;


public:
    /*
     * Setters
     */
    void print() const;
    void setCallId(ulong);
    void setCallerId(ulong);
    void setCalleeId(ulong);
    void setCost(double);
    void setDuration(uint);
    void setLongDistance(bool);
    void setPlaceCaller(ulong);
    void setPlaceCallee(ulong);
    void setTimestamp(Timestamp);
};
