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

#include <inttypes.h>
#include <set>
#include <string>

#include "server/sqlite/sqlite3.h"

#include "server/sep/aim_schema.h"
#include "server/sep/aim_schema_entry.h"
#include "server/sep/value.h"
#include "server/sep/window.h"

/*
 * Class responsible for building the schema of the Analytics Matrix. It reads
 * from a sqlite database and creates schema entries describing each one of the
 * AM attributes. It registers the necessary function pointers and stores the
 * information a schema entry needs by parsing the retrieved data.
 *
 * Sample Usage:    sqlite3 *conn;
 *                  sqlite3_open_v2("meta_db.db", &conn, SQLITE_OPEN_READONLY, NULL);
 *                  SchemaBuilder s_b();
 *                  Schema s = s_b(conn);
 */
class AIMSchemaBuilder
{
public:
    using SE<T> = AIMSchemaEntry<T>;
    AIMSchemaBuilder() = default;

    /*
     * It builds the Analytics Matrix schema by parsing data being retrieved
     * from the database.
     */
    AIMSchema build(sqlite3 *conn);

private:
    AIMSchema _schema;

private:
    /*
     * It creates a new Value for an AM attribute.
     */
    Value buildValue(sqlite3_stmt *) const;

    /*
     * It creates a Window for an attribute.
     */
    Window buildWindow(sqlite3_stmt *) const;

    /*
     * Based on Value and Window Information it builds a schema entry.
     */
    SE buildSchemaEntry(const Value, const Window, sqlite3_stmt *);

    /*
     * These functions parse Value information.
     */
    AggrFun getAggrFun(const char *) const;
    tell::store::FieldType getDataType(const char *) const;
    Metric getMetric(const char *) const;

    /*
     * Parses filter information.
     */
    FilterType getFilterType(const char *) const;

    /*
     * These functions parse Window information.
     */
    WindowType getWindowType(const char *) const;
    WindowLength getWindowSize(const char *) const;

    /*
     * Parses data from the db and results in the respective filter function.
     */
    SE::FilterFPtr getFilter(const char *) const;

    /*
     * These functions set the correct values to function pointers,
     * e.g. init = initSum, update = updateSum.
     */
    void setFunctions(const Value, SE::InitDefFPtr*, SE::InitFPtr*,
                      SE::UpdateFPtr*, SE::MaintainFPtr*) const;
    void setMinMetric(const Metric, SE::InitDefFPtr*, SE::InitFPtr*,
                      SE::UpdateFPtr*, SE::MaintainFPtr*) const;
    void setMaxMetric(const Metric, SE::InitDefFPtr*, SE::InitFPtr*,
                      SE::UpdateFPtr*, SE::MaintainFPtr*) const;
    void setSumMetric(const Metric, SE::InitDefFPtr*, SE::InitFPtr*,
                      SE::UpdateFPtr*, SE::MaintainFPtr*) const;
};
