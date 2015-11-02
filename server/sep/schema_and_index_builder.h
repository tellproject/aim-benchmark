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

#include "util/sqlite/sqlite3.h"

#include "sep/aim_schema_builder.h"
#include "sep/campaign_index_builder.h"

/*
 * This class is responsible for reading the meta-database, building the AM
 * Schema (description of its attributes) and also building the Campaign Index.
 * For communicating with the mysql database a connection is used.
 *
 * _schema_builder         = responsible for building the AM schema.
 *
 * _campaign_index_builder = builds the campaign index.
 *
 * Sample Usage: SchemaAndIndexBuilder builder("db", "server", "user", "pass");
 *               Schema s = builder.buildSchema();
 *               CampaignIndex c_i = builder.buildCampaignIndex(s);
 */
class SchemaAndIndexBuilder
{
public:
    SchemaAndIndexBuilder(const char *db);
    ~SchemaAndIndexBuilder();

public:
    /*
     * This function reads all the necessary information regarding the AM
     * attributes from the meta-database and it builds the AM schema.
     */
    AIMSchema buildAIMSchema();

    /*
     * It reads the meta-database, it parses the retrieved data and based on
     * them and also on the Schema it builds the Campaign Index (predicates,
     * unindexed conjuncts, campaigns and entry indexes).
     */
    CampaignIndex buildCampaignIndex(const AIMSchema&);

private:
    sqlite3 *_conn;
    AIMSchemaBuilder _aim_schema_builder;
    CampaignIndexBuilder _campaign_index_builder;
};
