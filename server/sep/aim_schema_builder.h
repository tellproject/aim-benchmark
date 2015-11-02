#pragma once

#include <inttypes.h>
#include <set>
#include <string>

#include "util/sqlite/sqlite3.h"

#include "sep/aim_schema.h"
#include "sep/aim_schema_entry.h"
#include "sep/value.h"
#include "sep/window.h"

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
    typedef AIMSchemaEntry SE;
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
    DataType getDataType(const char *) const;
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
