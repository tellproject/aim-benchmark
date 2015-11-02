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
#include "campaign.h"

Campaign::Campaign(uint16_t id, Timestamp from, Timestamp to, FiringInterval interval,
                   FiringStartCond start_cond, FireFPtr fireCheck): _id(id),
    _from(from), _to(to), _interval(interval), _start_cond(start_cond),
    _fireCheck(fireCheck) {}

void
Campaign::setId(uint16_t id)
{
    _id = id;
}

void
Campaign::setValidFrom(Timestamp from)
{
    _from = from;
}

void
Campaign::setValidTo(Timestamp to)
{
    _to = to;
}

void
Campaign::setFiringInterval(FiringInterval interval)
{
    _interval = interval;
}

void
Campaign::setFiringStartCond(FiringStartCond start_cond)
{
    _start_cond = start_cond;
}

void
Campaign::setFireCheck(FireFPtr fire_check)
{
    _fireCheck = fire_check;
}

/*
 * Firing policy: FiringInterval = Always
 */
bool
always(const Timestamp &last_fired, const Timestamp &current)
{
    return true;
}

/*
 * Firing policy: FiringInterval = One day, Firing Start Condtion = sliding
 */
bool
oneDaySliding(const Timestamp &last_fired, const Timestamp &current)
{
    return ((current - last_fired) >= MSECS_PER_DAY);
}

/*
 * Firing policy: FiringInterval = Two days, Firing Start Condtion = sliding
 */
bool
twoDaySliding(const Timestamp &last_fired, const Timestamp &current)
{
    return ((current - last_fired) >= 2 * MSECS_PER_DAY);
}

/*
 * Firing policy: FiringInterval = One week, Firing Start Condtion = sliding
 */
bool
oneWeekSliding(const Timestamp &last_fired, const Timestamp &current)
{
    return ((current - last_fired) >= MSECS_PER_WEEK);
}

/*
 * Firing policy: FiringInterval = One day, Firing Start Condtion = fixed
 */
bool
oneDayFixed(const Timestamp &last_fired, const Timestamp &current)
{
    return (current >= ((last_fired / MSECS_PER_DAY) + 1) * MSECS_PER_DAY);
}

/*
 * Firing policy: FiringInterval = Two days, Firing Start Condtion = fixed
 */
bool
twoDayFixed(const Timestamp &last_fired, const Timestamp &current)
{
    return (current >= ((last_fired / MSECS_PER_DAY) + 2) * MSECS_PER_DAY);
}

/*
 * Firing policy: FiringInterval = One week, Firing Start Condtion = fixed
 */
bool
oneWeekFixed(const Timestamp &last_fired, const Timestamp &current)
{
    Timestamp interval_end = (last_fired - FIRST_MONDAY) / MSECS_PER_WEEK;
    interval_end = (interval_end + 1) * MSECS_PER_WEEK + FIRST_MONDAY;
    return (current >= interval_end);
}
