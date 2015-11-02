#pragma once

enum class DimensionAttribute {
    SUBSCRIPTION_TYPE,
    SUBSCRIPTION_COST,
    SUBSCRIPTION_FREE_CALL_MINS,
    SUBSCRIPTION_DATA,
    REGION_ZIP,
    REGION_CITY,
    REGION_STATE,
    REGION_COUNTRY,
    REGION_REGION,
    SUBSCRIBER_CATEGORY_TYPE,
    SUBSCRIBER_VALUE_TYPE,
    SUBSCRIBER_VALUE_THRESHOLD,
    SUBSCRIBER_ID,
    count
};

template<int FAKE=0>
uint16_t dimension_attribute_size(DimensionAttribute attr) {
    static_assert(FAKE == 0, "this is not meant for changing");
    if (attr == DimensionAttribute::SUBSCRIBER_ID) {
        return 8;
    }
    return 2;
}
