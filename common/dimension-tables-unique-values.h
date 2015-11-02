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

