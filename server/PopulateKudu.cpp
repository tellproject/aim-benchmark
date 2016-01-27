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
#include "PopulateKudu.hpp"
#include <chrono>
#include <algorithm>

#include "kudu.hpp"

#include "telldb/Field.hpp"

#include <common/Util.hpp>
#include <common/dimension-tables.h>
#include <common/dimension-tables-mapping.h>

namespace aim {

namespace { // anonymous namesapce

inline void initializeWideTableColumn (std::unordered_map<crossbow::string, tell::db::Field> &tuple,
                     const AIMSchema &aimSchema) {
    for (unsigned i = 0; i < aimSchema.numOfEntries(); ++i)
    {
        tuple.emplace(std::make_pair(aimSchema[i].name(), aimSchema[i].initDef()));
    }
}

}   // anonymous namespace

using namespace kudu::client;

void Populator::populateWideTable(kudu::client::KuduSession& session,
            const AIMSchema &aimSchema,
            uint64_t lowest, uint64_t highest)
{
    Random_t rand;
    std::tr1::shared_ptr<KuduTable> table;
    assertOk(session.client()->OpenTable("wt", &table));

    std::unordered_map<crossbow::string, tell::db::Field> tuple;
    initializeWideTableColumn(tuple, aimSchema); // these attributes are the same for each tuple
    for (uint64_t i = lowest; i <= highest; ++i) {
        auto ins = table->NewInsert();
        auto row = ins->mutable_row();

        // subscriber-id and last-updated
        assertOk(row->SetInt64("subscriber_id", static_cast<int64_t>(i)));
        assertOk(row->SetInt64("last_updated", now()));

        // insert all standard attributes
        for (auto kvPair: tuple) {
            std::string fieldName(kvPair.first.c_str(), kvPair.first.size());
            switch (kvPair.second.type()) {
            case tell::store::FieldType::INT:
                assertOk(row->SetInt32(fieldName, kvPair.second.value<int32_t>()));
                break;
            case tell::store::FieldType::BIGINT:
                assertOk(row->SetInt64(fieldName, kvPair.second.value<int64_t>()));
                break;
            case tell::store::FieldType::DOUBLE:
                assertOk(row->SetDouble(fieldName, kvPair.second.value<double>()));
                break;
            default:
                const char * msg = "Error from Kudu Popluation: non-expected field type %1% for AIM wide-table attribute";
                LOG_ERROR(msg, kvPair.second.type());
                throw std::runtime_error(msg);
            }
        }

        // subscription type
        int16_t subscriptionId = rand.randomWithin<int16_t>(
                0, subscription_types.size()-1);
        assertOk(row->SetInt16("subscription_type_id", subscription_type_to_id[
                subscription_types[subscriptionId]]));
        assertOk(row->SetInt64("subscription_cost_id", subscription_cost_to_id[
                subscription_cost[subscriptionId]]));
        assertOk(row->SetInt64("subscription_free_call_mins_id",
                subscription_free_call_mins_to_id[subscription_free_call_mins[
                subscriptionId]]));
        assertOk(row->SetInt64("subscription_data_id", subscription_data_to_id[
                subscription_data[subscriptionId]]));

        // city
        int16_t zipId = rand.randomWithin<int16_t>(
                0, region_zip.size()-1);
        assertOk(row->SetInt16("city_zip", region_zip_to_id[
                region_zip[zipId]]));
        assertOk(row->SetInt16("region_cty_id", region_city_to_id[
                region_city[zipId]]));
        assertOk(row->SetInt16("region_state_id", region_state_to_id[
                region_state[zipId]]));
        assertOk(row->SetInt16("region_country_id", region_country_to_id[
                region_country[zipId]]));
        assertOk(row->SetInt16("region_region_id", region_region_to_id[
                region_region[zipId]]));

        // category
        int16_t categoryId = rand.randomWithin<int16_t>(
                0, subscriber_category_type.size()-1);
        assertOk(row->SetInt16("category_id", subscriber_category_type_to_id[
                subscriber_category_type[categoryId]]));

        // value type
        int16_t valueTypeId = rand.randomWithin<int16_t>(
                0, subscriber_value_type.size()-1);
        assertOk(row->SetInt16("value_type_id", subscriber_value_type_to_id[
                               subscriber_value_type[valueTypeId]]));
        assertOk(row->SetInt16("value_type_threshold_id", subscriber_value_threshold_to_id[
                subscriber_value_threshold[valueTypeId]]));

        assertOk(session.Apply(ins));
    }
    assertOk(session.Flush());
}

} // namespace aim
