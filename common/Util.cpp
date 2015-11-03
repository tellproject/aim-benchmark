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
#include "Util.hpp"

#include "common/dimension-tables-unique-values.h"

namespace crossbow {

template class singleton<aim::Random_t, create_static<aim::Random_t>, default_lifetime<aim::Random_t>>;

} // namespace crossbow

namespace aim {

Random_t::Random_t(size_t subscriberNum, uint8_t workloadSize) :
    _double_distr(0.1, 100.0),
    _uint_distr(1, 10000),
    _ulong_distr(1, subscriberNum),
    _bool_distr(0, 1),
    _query_dist(0, workloadSize),
    _q124_dist(2, 10),
    _q4_b_dist(200000, 1500000),
    _q5_a_dist(0, subscription_unique_type.size()-1),
    _q5_b_dist(0, subscriber_unique_category_type.size()-1),
    _q6_country_dist(0, region_unique_country.size()-1),
    _q7_subscr_value_type_dist(0, subscriber_unique_value_type.size()-1)
{

}

Event
Random_t::randomEvent()
{
    // call-id and timestamp set to current time
    auto ts = now();
    Event e;
    e.setCallId(ts);
    e.setCalleeId(_ulong_distr(mRandomDevice));
    e.setCallerId(_ulong_distr(mRandomDevice));
    e.setTimestamp(ts);
    e.setDuration(_uint_distr(mRandomDevice));
    e.setCost(_double_distr(mRandomDevice));
    e.setLongDistance((_bool_distr(mRandomDevice)));
    e.setPlaceCaller(_ulong_distr(mRandomDevice));
    e.setPlaceCallee(_ulong_distr(mRandomDevice));
    return e;
}

uint8_t Random_t::randomQuery()
{
    return _query_dist(mRandomDevice);
}

void Random_t::randomQ1(Q1In &arg)
{
    arg.alpha = _q124_dist(mRandomDevice);
}

void Random_t::randomQ2(Q2In &arg)
{
    arg.alpha = _q124_dist(mRandomDevice);
}

void Random_t::randomQ4(Q4In &arg)
{
    arg.alpha = _q124_dist(mRandomDevice);
    arg.beta = _q4_b_dist(mRandomDevice);
}


void Random_t::randomQ5(Q5In &arg)
{
    arg.sub_type = _q5_a_dist(mRandomDevice);
    arg.sub_category = _q5_b_dist(mRandomDevice);
}

void Random_t::randomQ6(Q6In &arg)
{
    arg.country_id = _q6_country_dist(mRandomDevice);

}

void Random_t::randomQ7(Q7In &arg)
{
    arg.subscriber_value_type = _q7_subscr_value_type_dist(mRandomDevice);
    arg.window_length = _bool_distr(mRandomDevice);
}

int64_t now() {
    auto now = std::chrono::system_clock::now();
    return now.time_since_epoch().count();
}

} // namespace aim

