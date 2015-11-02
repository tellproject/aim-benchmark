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

#include <inttypes.h>

#include "sep/utils.h"

/*
 * This class simulates a campaign. It contains all the necessary information
 * for describing a campaign (validity period and firing policy). We opted to
 * store conjuncts and predicates outside of campaigns. A function pointer is
 * used for checking swiftly if a campaign can fire.
 *
 * Sample Usage: Campaign c(id, from, to, interval, start_cond, fireCheck);
 *               bool result = c.canFire(last_fired, current_timestamp);
 */
class Campaign
{
public:
    typedef bool (*FireFPtr)(const Timestamp &last_fired, const Timestamp &current);
    Campaign() = default;
    Campaign(uint16_t id, Timestamp from, Timestamp to, FiringInterval interval,
             FiringStartCond start_cond, FireFPtr fireCheck);

public:
    bool canFire(const Timestamp &last_fired, const Timestamp &current) const;
    void setId(uint16_t);
    void setValidFrom(Timestamp);
    void setValidTo(Timestamp);
    void setFiringInterval(FiringInterval);
    void setFiringStartCond(FiringStartCond);
    void setFireCheck(FireFPtr);

private:
    uint16_t _id;
    Timestamp _from;
    Timestamp _to;
    FiringInterval _interval;
    FiringStartCond _start_cond;
    /*
     * Function pointer is registered during campaign index building. Based on
     * this pointer we can determine whether a campaign can fire without
     * switching between the different cases for Firing Interval and Firing
     * Start Condition.
     */
    FireFPtr _fireCheck;

private:
    bool isActive(Timestamp timestamp) const;
};

/*
 * Checks whether the campaign is active.
 */
inline bool
Campaign::isActive(Timestamp timestamp) const
{
    return (timestamp >= _from && timestamp <= _to);
}

/*
 * Checks whether the campaign can fire based on its validity period and its
 * firing policy.
 */
inline bool
Campaign::canFire(const Timestamp &last_fired, const Timestamp &current) const
{
    return (isActive(current) && _fireCheck(last_fired, current));
}

/*
 * Functions implementing the firing policy of a campaign. They encapsulate
 * both firing interval and firing start condition.
 */
bool
always(const Timestamp &last_fired, const Timestamp &current);

bool
oneDaySliding(const Timestamp &last_fired, const Timestamp &current);

bool
twoDaySliding(const Timestamp &last_fired, const Timestamp &current);

bool
oneWeekSliding(const Timestamp &last_fired, const Timestamp &current);

bool
oneDayFixed(const Timestamp &last_fired, const Timestamp &current);

bool
twoDayFixed(const Timestamp &last_fired, const Timestamp &current);

bool
oneWeekFixed(const Timestamp &last_fired, const Timestamp &current);
