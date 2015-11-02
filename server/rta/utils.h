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
