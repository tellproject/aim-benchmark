#pragma once

#include <string>
#include <unordered_map>

/*
 * Mappings for Subscription Type Dimension Table.
 */
std::unordered_map<std::string, uint16_t> subscription_type_to_id
{{"prepaid", 0}, {"contract", 1}};

std::unordered_map<std::string, uint16_t> subscription_cost_to_id
{{"0", 0}, {"10", 1}, {"20", 2}, {"50", 3}};

std::unordered_map<std::string, uint16_t> subscription_free_call_mins_to_id
{{"0", 0}, {"120", 1}, {"720", 2}, {"unlimited", 3}};

std::unordered_map<std::string, uint16_t> subscription_data_to_id
{{"0", 0}, {"10", 1}, {"50", 2}, {"unlimited", 3}};

/*
 * Mappings for Region Info Dimension Table.
 */
std::unordered_map<std::string, uint16_t> region_zip_to_id
{{"CH-1000", 0}, {"CH-8000", 1}, {"DE-80801", 2}, {"ARG-B6500", 3},
 {"CHI-100000", 4}, {"CHI-101500", 5}};

std::unordered_map<std::string, uint16_t> region_city_to_id
{{"Lausanne", 0}, {"Zurich", 1}, {"Munich", 2},
 {"Buenos Aires", 3}, {"Beijing", 4}};

std::unordered_map<std::string, uint16_t> region_state_to_id
{{"Vaud", 0}, {"Zurich", 1}, {"Bayern", 2}, {"Buenos Aires", 3}, {"Beijing", 4}};

std::unordered_map<std::string, uint16_t> region_country_to_id
{{"Switzerland", 0}, {"Germany", 1}, {"Argentina", 2}, {"China", 3}};

std::unordered_map<std::string, uint16_t> region_region_to_id
{{"EUROPE", 0}, {"SOUTH AMERICA", 1}, {"ASIA", 2}};

/*
 * Mappings for Subscriber Category Table.
 */
std::unordered_map<std::string, uint16_t> subscriber_category_type_to_id
{{"business", 0}, {"private", 1}, {"company", 2}};

/*
 * Mappings for Subscriber Value Table.
 */
std::unordered_map<std::string, uint16_t> subscriber_value_type_to_id
{{"none", 0}, {"silver", 1}, {"gold", 2}, {"platinum", 3}};

std::unordered_map<std::string, uint16_t> subscriber_value_threshold_to_id
{{"0", 0}, {"30", 1}, {"80", 2}, {"150", 3}};
