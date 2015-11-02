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
#include "dimension-tables-unique-values.h"

/*
 * Unique values of Dimension Table Columns.
 *
 * Subscription Type Dimension Table.
 */
std::array<std::string, 2> subscription_unique_type
{{"prepaid", "contract"}};

std::array<std::string, 4> subscription_unique_cost
{{"0", "10", "20", "50"}};

std::array<std::string, 4> subscription_unique_free_call_mins
{{"0", "120", "720", "unlimited"}};

std::array<std::string, 4> subscription_unique_data
{{"0", "10", "50", "unlimited"}};

/*
 * Region Info Dimension Table.
 */
std::array<std::string, 6> region_unique_zip
{{"CH-1000", "CH-8000", "DE-80801", "ARG-B6500", "CHI-100000", "CHI-101500"}};

std::array<std::string, 5> region_unique_city
{{"Lausanne", "Zurich", "Munich", "Buenos Aires", "Beijing"}};

std::array<std::string, 5> region_unique_state
{{"Vaud", "Zurich", "Bayern", "Buenos Aires", "Beijing"}};

std::array<std::string, 4> region_unique_country
{{"Switzerland", "Germany", "Argentina", "China"}};

std::array<std::string, 3> region_unique_region
{{"EUROPE", "SOUTH AMERICA", "ASIA"}};

/*
 * Subscriber Category Table.
 */
std::array<std::string, 3> subscriber_unique_category_type
{{"business", "private", "company"}};

/*
 * Subscriber Value Table.
 */
std::array<std::string, 4> subscriber_unique_value_type
{{"none", "silver", "gold", "platinum"}};

std::array<std::string, 4> subscriber_unique_value_threshold
{{"0", "30", "80", "150"}};
