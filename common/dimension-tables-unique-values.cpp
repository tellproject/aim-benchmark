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
