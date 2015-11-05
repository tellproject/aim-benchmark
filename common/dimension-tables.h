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

#include <array>
#include <string>

/*
 * Subscription Type Dimension Table.
 */
extern std::array<std::string, 4> subscription_types;

extern std::array<std::string, 4> subscription_cost;

extern std::array<std::string, 4> subscription_free_call_mins;

extern std::array<std::string, 4> subscription_data;

/*
 * Region Info Dimension Table.
 */
extern  std::array<std::string, 6> region_zip;

extern std::array<std::string, 6> region_city;

extern std::array<std::string, 6> region_state;


extern std::array<std::string, 6> region_country;

extern std::array<std::string, 6> region_region;
/*
 * Subscriber Category Table.
 */
extern std::array<std::string, 3> subscriber_category_type;

/*
 * Subscriber Value Table.
 */
extern std::array<std::string, 4> subscriber_value_type;

extern std::array<std::string, 4> subscriber_value_threshold;
