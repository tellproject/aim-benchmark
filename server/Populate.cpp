/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and
 * others.
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
#include "Populate.hpp"
#include <telldb/Transaction.hpp>
#include <chrono>
#include <algorithm>
#include <unordered_map>
#include <limits>

#include <common/Util.hpp>
#include <common/dimension-tables.h>
#include <common/dimension-tables-mapping.h>

using namespace tell::db;

namespace aim {

namespace { // anonymous namesapce

/**
 * TODO: this is kind of a hack, but it works... refactor it sometime!
 */
inline void addField(std::unordered_map<crossbow::string, Field> &tuple,
                     char *tmp, DataType type,
                     crossbow::string fieldName) {
    switch (type) {
    case DataType::ULONG:
    {
        uint64_t u1 = *(reinterpret_cast<uint64_t*>(tmp));
        if (u1 == std::numeric_limits<uint64_t>::min()) {
            tuple [fieldName] = std::numeric_limits<int64_t>::min();
        } else if (u1 == std::numeric_limits<uint64_t>::max()) {
            tuple [fieldName] = std::numeric_limits<int64_t>::max();
        } if (u1 == 0) {
            tuple [fieldName] = 0;
        } else {
            LOG_ASSERT(false, "impossible ulong value at initialization!")
        }
        break;
    }
    case DataType::UINT:
    {
        uint32_t i1 = *(reinterpret_cast<uint32_t*>(tmp));
        if (i1 == std::numeric_limits<uint32_t>::min()) {
            tuple [fieldName] = std::numeric_limits<int32_t>::min();
        } else if (i1 == std::numeric_limits<uint32_t>::max()) {
            tuple [fieldName] = std::numeric_limits<int32_t>::max();
        } if (i1 == 0) {
            tuple [fieldName] = 0;
        } else {
            LOG_ASSERT(false, "impossible uint value at initialization!")
        }
        break;
    }
    case DataType::DOUBLE:
    {
        tuple [fieldName] = *(reinterpret_cast<double*>(tmp));
        break;
    }
    case DataType::INT:
    {
        tuple [fieldName] = *(reinterpret_cast<int32_t*>(tmp));
    }
    }
}

inline void initializeWideTableColumn (std::unordered_map<crossbow::string, Field> &tuple,
                     const AIMSchema &aimSchema,
                     const DimensionSchema &dimSchema) {
    char *tmp = new char [8];
    for (unsigned i = 0; i < aimSchema.numOfEntries(); ++i)
    {
        aimSchema[i].initDef(&tmp);
        addField(tuple, tmp, aimSchema[i].type(), "a_" + crossbow::to_string(i));
    }
}

}   // anonymous namespace

void Populator::populateWideTable(tell::db::Transaction &transaction,
                    const AIMSchema &aimSchema, const DimensionSchema &dimSchema,
                    uint64_t lowest, uint64_t highest) {
    Random_t rand;
    auto tIdFuture = transaction.openTable("wt");
    auto tId = tIdFuture.get();
    std::unordered_map<crossbow::string, Field> tuple;
    initializeWideTableColumn(tuple, aimSchema, dimSchema); // these attributes are the same for each tuple
    for (uint64_t i = lowest; i <= highest; ++i) {
        tuple["subscriber_id"] = Field(static_cast<int64_t>(i));
        tuple["last_updated"] = Field(now());

        // subscription type
        uint16_t subscriptionType = rand.randomWithin<int32_t>(0, subscription_types.size()-1);
        tuple["subscription_type_id"] = Field(subscription_type_to_id[subscription_types[subscriptionType]]);

        //TODO: continue here!!!
        tuple["subscription_cost_id"] = Field(rand.randomWithin<int32_t>(0, subscription_type_to_id.size()));
        tuple["subscription_free_call_mins_id"] = Field(rand.randomWithin<int32_t>(0, subscription_type_to_id.size()));
        tuple["subscription_data_id"] = Field(rand.randomWithin<int32_t>(0, subscription_type_to_id.size()));

        tuple["city_zip"] = Field(rand.randomWithin<int32_t>(0, subscription_type_to_id.size()));
        tuple["region_cty_id"] = Field(rand.randomWithin<int32_t>(0, subscription_type_to_id.size()));
        tuple["region_state_id"] = Field(rand.randomWithin<int32_t>(0, subscription_type_to_id.size()));

        tuple["category_id"] = Field(rand.randomWithin<int32_t>(0, subscription_type_to_id.size()));

        tuple["value_type_id"] = Field(rand.randomWithin<int32_t>(0, subscription_type_to_id.size()));
        tuple["value_type_threshold_id"] = Field(rand.randomWithin<int32_t>(0, subscription_type_to_id.size()));
        transaction.insert(tId, tell::db::key_t{uint64_t(i)}, tuple);
    }
}

} // namespace aim
