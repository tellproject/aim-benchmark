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

#include <string>
#include <unordered_map>

/*
 * Mappings for Subscription Type Dimension Table.
 */
extern std::unordered_map<std::string, uint16_t> subscription_type_to_id;

extern std::unordered_map<std::string, uint16_t> subscription_cost_to_id;

extern std::unordered_map<std::string, uint16_t> subscription_free_call_mins_to_id;

extern std::unordered_map<std::string, uint16_t> subscription_data_to_id;

/*
 * Mappings for Region Info Dimension Table.
 */
extern std::unordered_map<std::string, uint16_t> region_zip_to_id;

extern std::unordered_map<std::string, uint16_t> region_city_to_id;

extern std::unordered_map<std::string, uint16_t> region_state_to_id;

extern std::unordered_map<std::string, uint16_t> region_country_to_id;

extern std::unordered_map<std::string, uint16_t> region_region_to_id;

/*
 * Mappings for Subscriber Category Table.
 */
extern std::unordered_map<std::string, uint16_t> subscriber_category_type_to_id;

/*
 * Mappings for Subscriber Value Table.
 */
extern std::unordered_map<std::string, uint16_t> subscriber_value_type_to_id;

extern std::unordered_map<std::string, uint16_t> subscriber_value_threshold_to_id;
