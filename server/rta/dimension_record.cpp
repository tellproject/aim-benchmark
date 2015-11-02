#include "rta/dimension_record.h"

#include <cstring>
#include <cassert>

#include "util/dimension-tables-mapping.h"
#include "util/dimension-tables.h"

/*
 * Dummy Constructor, it constructs a predefined dimension record
 * for a subscriber.
 */
DimensionRecord::DimensionRecord(uint64_t subscriber_id, const DimensionSchema &schema)
{
    std::vector<std::string> values{"prepaid", "10", "120", "50", "CH-8000",
                                    "Munich", "Bayern", "Germany", "EUROPE",
                                    "private", "silver", "30"};
    _fillIn(schema, values, subscriber_id);
}

/*
 * Dummy Constructor, it constructs a dimension record for a subscriber
 * using random values.
 */
DimensionRecord::DimensionRecord(uint64_t subscriber_id, const DimensionSchema &schema,
                                 std::mt19937 &eng,
                                 std::uniform_int_distribution<uint> &distr)
{
    std::vector<std::string> values;
    values.reserve(schema.numOfEntries());

    uint subscription_type_pk = distr(eng) % subscription_types.size();
    uint region_info_pk = distr(eng) % region_zip.size();
    uint subscriber_category_pk = distr(eng) % subscriber_category_type.size();
    uint subscriber_value_pk = distr(eng) % subscriber_value_type.size();

    values.push_back(subscription_types[subscription_type_pk]);
    values.push_back(subscription_cost[subscription_type_pk]);
    values.push_back(subscription_free_call_mins[subscription_type_pk]);
    values.push_back(subscription_data[subscription_type_pk]);
    values.push_back(region_zip[region_info_pk]);
    values.push_back(region_city[region_info_pk]);
    values.push_back(region_state[region_info_pk]);
    values.push_back(region_country[region_info_pk]);
    values.push_back(region_region[region_info_pk]);
    values.push_back(subscriber_category_type[subscriber_category_pk]);
    values.push_back(subscriber_value_type[subscriber_value_pk]);
    values.push_back(subscriber_value_threshold[subscriber_value_pk]);
    _fillIn(schema, values, subscriber_id);
}

void
DimensionRecord::_fillIn(const DimensionSchema &schema,
                         std::vector<std::string> values, uint64_t subscriber_id)
{
    assert(schema.numOfEntries() == values.size()+1);

    _data.resize(schema.size() + sizeof(subscriber_id));
    memset(_data.data(), '0', schema.size());
    char *tmp = _data.data();

    uint16_t id = subscription_type_to_id[values[0]];
    memcpy(tmp, &id, schema.sizeAt(0));
    tmp += schema.sizeAt(0);

    id = subscription_cost_to_id[values[1]];
    memcpy(tmp, &id, schema.sizeAt(1));
    tmp += schema.sizeAt(1);

    id = subscription_free_call_mins_to_id[values[2]];
    memcpy(tmp, &id, schema.sizeAt(2));
    tmp += schema.sizeAt(2);

    id = subscription_data_to_id[values[3]];
    memcpy(tmp, &id, schema.sizeAt(3));
    tmp += schema.sizeAt(3);

    id = region_zip_to_id[values[4]];
    memcpy(tmp, &id, schema.sizeAt(4));
    tmp += schema.sizeAt(4);

    id = region_city_to_id[values[5]];
    memcpy(tmp, &id, schema.sizeAt(5));
    tmp += schema.sizeAt(5);

    id = region_state_to_id[values[6]];
    memcpy(tmp, &id, schema.sizeAt(6));
    tmp += schema.sizeAt(6);

    id = region_country_to_id[values[7]];
    memcpy(tmp, &id, schema.sizeAt(7));
    tmp += schema.sizeAt(7);

    id = region_region_to_id[values[8]];
    memcpy(tmp, &id, schema.sizeAt(8));
    tmp += schema.sizeAt(8);

    id = subscriber_category_type_to_id[values[9]];
    memcpy(tmp, &id, schema.sizeAt(9));
    tmp += schema.sizeAt(9);

    id = subscriber_value_type_to_id[values[10]];
    memcpy(tmp, &id, schema.sizeAt(10));
    tmp += schema.sizeAt(10);

    id = subscriber_value_threshold_to_id[values[11]];
    memcpy(tmp, &id, schema.sizeAt(11));
    tmp += schema.sizeAt(11);

    memcpy(tmp, &subscriber_id, sizeof(subscriber_id));
    tmp += sizeof(subscriber_id);
}

const std::vector<char>&
DimensionRecord::data() const
{
    return _data;
}
