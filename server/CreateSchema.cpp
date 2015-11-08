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
#include "CreateSchema.hpp"
#include <telldb/Transaction.hpp>

namespace aim {

using namespace tell;

void createSchema(tell::db::Transaction& transaction, const AIMSchema &aimSchema) {
    store::Schema schema(store::TableType::TRANSACTIONAL);
    // Primary key: subscriber_id
    schema.addField(store::FieldType::BIGINT, "subscriber_id", true);
    schema.addField(store::FieldType::BIGINT, "last_updated", true);

    // wide table columns
    for (unsigned i = 0; i < aimSchema.numOfEntries(); ++i)
        schema.addField(aimSchema[i].type(), aimSchema[i].name(), true);

    // dimension columns
    schema.addField(store::FieldType::SMALLINT, "subscription_type_id", true);
    schema.addField(store::FieldType::SMALLINT, "subscription_cost_id", true);
    schema.addField(store::FieldType::SMALLINT, "subscription_free_call_mins_id", true);
    schema.addField(store::FieldType::SMALLINT, "subscription_data_id", true);

    schema.addField(store::FieldType::SMALLINT, "city_zip", true);
    schema.addField(store::FieldType::SMALLINT, "region_cty_id", true);
    schema.addField(store::FieldType::SMALLINT, "region_state_id", true);
    schema.addField(store::FieldType::SMALLINT, "region_country_id", true);
    schema.addField(store::FieldType::SMALLINT, "region_region_id", true);

    schema.addField(store::FieldType::SMALLINT, "category_id", true);
    schema.addField(store::FieldType::SMALLINT, "value_type_id", true);
    schema.addField(store::FieldType::SMALLINT, "value_type_threshold_id", true);

    transaction.createTable("wt", schema);
}

} // namespace aim
