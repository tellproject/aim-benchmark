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
#include "CreateSchemaKudu.hpp"
#include "kudu.hpp"
#include <thread>

namespace aim {

namespace {

enum class FieldType {
    SMALLINT, INT, BIGINT, FLOAT, DOUBLE, TEXT
};

using namespace kudu::client;

void addField(kudu::client::KuduSchemaBuilder& schemaBuilder, FieldType type, const std::string& name, bool notNull) {
    auto col = schemaBuilder.AddColumn(name);
    switch (type) {
    case FieldType::SMALLINT:
        col->Type(kudu::client::KuduColumnSchema::INT16);
        break;
    case FieldType::INT:
        col->Type(kudu::client::KuduColumnSchema::INT32);
        break;
    case FieldType::BIGINT:
        col->Type(kudu::client::KuduColumnSchema::INT64);
        break;
    case FieldType::FLOAT:
        col->Type(kudu::client::KuduColumnSchema::FLOAT);
        break;
    case FieldType::DOUBLE:
        col->Type(kudu::client::KuduColumnSchema::DOUBLE);
        break;
    case FieldType::TEXT:
        col->Type(kudu::client::KuduColumnSchema::STRING);
        break;
    }
    if (notNull) {
        col->NotNull();
    } else {
        col->Nullable();
    }
}

std::vector<const kudu::KuduPartialRow*> addSplitsSubscribers(int64_t subscriberNum, int partitions, kudu::client::KuduSchema& schema) {
    std::vector<const kudu::KuduPartialRow*> splits;
    int64_t increment = subscriberNum / partitions;
    for (int64_t i = 1; i < partitions; ++i) {
        auto row = schema.NewRow();
        assertOk(row->SetInt64(0, i*increment));
        splits.emplace_back(row);
    }
    return splits;
}

template<class Fun>
void createTable(kudu::client::KuduSession& session, kudu::client::KuduSchemaBuilder& schemaBuilder, const std::string& name, const std::vector<std::string>& rangePartitionColumns, Fun generateSplits) {
    std::unique_ptr<kudu::client::KuduTableCreator> tableCreator(session.client()->NewTableCreator());
    kudu::client::KuduSchema schema;
    assertOk(schemaBuilder.Build(&schema));
    tableCreator->table_name(name);
    tableCreator->schema(&schema);
    tableCreator->num_replicas(1);
    auto splits = generateSplits(schema);
    //tableCreator->set_range_partition_columns(rangePartitionColumns);
    tableCreator->split_rows(splits);
    assertOk(tableCreator->Create());
    tableCreator.reset(nullptr);
}

FieldType fromTellStoreFieldType(tell::store::FieldType type) {
    switch (type) {
    case tell::store::FieldType::BIGINT:
        return FieldType::BIGINT;
    case tell::store::FieldType::INT:
        return FieldType::INT;
    case tell::store::FieldType::DOUBLE:
        return FieldType::DOUBLE;
    default:
        // AIM Schema is always one of the three above!
        assert(false);
        return FieldType::INT;
    }
}

} // anonymous namespace

void createSchema(kudu::client::KuduSession& session, const int64_t subscriberNum, const AIMSchema &aimSchema, int partitions)
{
    kudu::client::KuduSchemaBuilder schemaBuilder;
    // Primary key: subscriber_id
    addField(schemaBuilder, FieldType::BIGINT, "subscriber_id", true);
    addField(schemaBuilder, FieldType::BIGINT, "last_updated", true);

    // wide table columns
    for (unsigned i = 0; i < aimSchema.numOfEntries(); ++i)
        addField(schemaBuilder, fromTellStoreFieldType(aimSchema[i].type()),
                std::string(aimSchema[i].name().c_str(), aimSchema[i].name().size()), true);

    // dimension columns
    addField(schemaBuilder, FieldType::SMALLINT, "subscription_type_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "subscription_cost_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "subscription_free_call_mins_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "subscription_data_id", true);

    addField(schemaBuilder, FieldType::SMALLINT, "city_zip", true);
    addField(schemaBuilder, FieldType::SMALLINT, "region_cty_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "region_state_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "region_country_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "region_region_id", true);

    addField(schemaBuilder, FieldType::SMALLINT, "category_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "value_type_id", true);
    addField(schemaBuilder, FieldType::SMALLINT, "value_type_threshold_id", true);

    schemaBuilder.SetPrimaryKey({"subscriber_id"});
    using namespace std::placeholders;
    createTable(session, schemaBuilder, "wt", {"subscriber_id"}, std::bind(&addSplitsSubscribers, subscriberNum, partitions, _1));
}

} // namespace aim
