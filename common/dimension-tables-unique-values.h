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
 * Unique values of Dimension Table Columns.
 *
 * Subscription Type Dimension Table.
 */
extern std::array<std::string, 2> subscription_unique_type;

extern std::array<std::string, 4> subscription_unique_cost;

extern std::array<std::string, 4> subscription_unique_free_call_mins;

extern std::array<std::string, 4> subscription_unique_data;

/*
 * Region Info Dimension Table.
 */
extern std::array<std::string, 6> region_unique_zip;

extern std::array<std::string, 5> region_unique_city;

extern std::array<std::string, 5> region_unique_state;


extern std::array<std::string, 4> region_unique_country;

extern std::array<std::string, 3> region_unique_region;

/*
 * Subscriber Category Table.
 */
extern std::array<std::string, 3> subscriber_unique_category_type;

/*
 * Subscriber Value Table.
 */
extern std::array<std::string, 4> subscriber_unique_value_type;

extern std::array<std::string, 4> subscriber_unique_value_threshold;

