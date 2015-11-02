#pragma once

#include <array>
#include <string>

/*
 * Subscription Type Dimension Table.
 */
std::array<std::string, 4> subscription_types{{"prepaid", "contract",
                                              "contract", "contract"}};

std::array<std::string, 4> subscription_cost{{"0", "10", "20", "50"}};

std::array<std::string, 4> subscription_free_call_mins{{"0", "120", "720",
                                                        "unlimited"}};

std::array<std::string, 4> subscription_data{{"0", "10", "50", "unlimited"}};

/*
 * Region Info Dimension Table.
 */
std::array<std::string, 6> region_zip{{"CH-1000", "CH-8000", "DE-80801",
                                       "ARG-B6500", "CHI-100000",
                                       "CHI-101500"}};

std::array<std::string, 6> region_city{{"Lausanne", "Zurich", "Munich",
                                        "Buenos Aires", "Beijing", "Beijing"}};

std::array<std::string, 6> region_state{{"Vaud", "Zurich", "Bayern",
                                         "Buenos Aires", "Beijing", "Beijing"}};


std::array<std::string, 6> region_country{{"Switzerland", "Switzerland",
                                           "Germany", "Argentina", "China",
                                           "China"}};

std::array<std::string, 6> region_region{{"EUROPE", "EUROPE", "EUROPE",
                                          "SOUTH AMERICA", "ASIA", "ASIA"}};
/*
 * Subscriber Category Table.
 */
std::array<std::string, 3> subscriber_category_type{{"business", "private",
                                                     "company"}};

/*
 * Subscriber Value Table.
 */
std::array<std::string, 4> subscriber_value_type{{"none", "silver", "gold",
                                                  "platinum"}};

std::array<std::string, 4> subscriber_value_threshold{{"0", "30", "80",
                                                        "150"}};
