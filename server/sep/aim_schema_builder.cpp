#include "aim_schema_builder.h"

#include "sep/utils.h"

/*
 * SQL query for retrieving all the necessary information for an AM attribute
 * from the meta-database.
 */
const char* AM_ATTR = "SELECT wt.id, wt.window_type, wt.window_size, wt.aggr_fun, \
                       wt.aggr_data_type, m.name, m.data_type FROM wt_attribute wt, \
                       metric m WHERE wt.metric = m.id ORDER BY wt.id;";

/*
 * For every AM attribute we build a schema entry. We first create the Value
 * information and then the Window information for an entry and finally we
 * build the respective schema entry.
 */
AIMSchema
AIMSchemaBuilder::build(sqlite3 *conn)
{
    // prepare statement
    sqlite3_stmt *stmt;
    const char *pzTail;
    int rc = sqlite3_prepare_v2(conn, AM_ATTR, strlen(AM_ATTR), &stmt, &pzTail);
    assert(rc == SQLITE_OK);
    assert(stmt != NULL);

    // process statement
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        Value value = buildValue(stmt);
        Window window = buildWindow(stmt);
        AIMSchemaEntry se = buildSchemaEntry(value, window, stmt);
        _schema.addEntry(se);
    }
    assert(rc == SQLITE_DONE);
    rc = sqlite3_finalize(stmt);
    assert(rc == SQLITE_OK);
    return _schema;

// old mysql error handling code
//    std::cerr << "Failed to execute AM_ATTR: " << query.error() << std::endl;
//    assert(false);
//    return decltype(build(conn))();
}

inline const char * sqlite3_signed_column_text(sqlite3_stmt* stmt, int iCol)
{
    return reinterpret_cast<const char *>(sqlite3_column_text(stmt, iCol));
}

/*
 * Takes a Row read by the meta-database and it parses it for setting up the
 * proper Value information. Namely:
 *
 *  aggregation function = min, max, sum, avg
 *
 *  data type of aggregation = int, uint, long, ulong, double
 *
 *  metric = call, cost, dur, local call, non local call, local cost, etc.
 *
 *  data type of metric = int, uint, long, ulong, double
 *
 * All this information is important for calculating the size of a schema
 * entry and for setting the right values in the Value of an entry.
 */
Value
AIMSchemaBuilder::buildValue(sqlite3_stmt *stmt) const
{
    AggrFun aggr_fun = getAggrFun(sqlite3_signed_column_text(stmt, 3));
    DataType aggr_data_type = getDataType(sqlite3_signed_column_text(stmt, 4));
    Metric metric = getMetric(sqlite3_signed_column_text(stmt, 5));
    DataType metric_data_type = getDataType(sqlite3_signed_column_text(stmt, 6));
    Value value(aggr_fun, aggr_data_type, metric, metric_data_type);
    return value;
}

/*
 * Takes a Row read by the meta-database and it parses it for setting up the
 * proper Window information. That is:
 *
 *  window type = tumbling, stepwise and continuous
 *
 *  window size = one day, one week
 */
Window
AIMSchemaBuilder::buildWindow(sqlite3_stmt *stmt) const
{
    WindowType win_type = getWindowType(sqlite3_signed_column_text(stmt, 1));
    WindowLength win_size = getWindowSize(sqlite3_signed_column_text(stmt, 2));
    Window window(win_type, win_size);
    return window;
}

/*
 * Given a Window, a Value and a Row from the meta-database it creates an
 * entry. It parses the Row for deciding what the Filter should be. It also
 * uses the Value (especially it's metric part) to determine and register the
 * various function pointers.
 *
 * Filter = noFilter, localFilter, nonLocalFilter
 *
 * For the function pointers (init_def, init, update and maintain) many
 * alternatives exist. E.g. initSum, initMax, initMin
 *
 */
AIMSchemaEntry
AIMSchemaBuilder::buildSchemaEntry(const Value value, const Window window,
                                sqlite3_stmt *stmt)
{
    AIMSchemaEntry::InitDefFPtr init_def;
    AIMSchemaEntry::InitFPtr init;
    AIMSchemaEntry::UpdateFPtr update;
    AIMSchemaEntry::MaintainFPtr maintain;
    AIMSchemaEntry::FilterFPtr filter;

    FilterType filter_type = getFilterType(sqlite3_signed_column_text(stmt, 5));
    filter = getFilter(sqlite3_signed_column_text(stmt, 5));
    //register filter and other functions
    setFunctions(value, &init_def, &init, &update, &maintain);
    AIMSchemaEntry schema_entry(value, window, init_def, init, update, maintain,
                                filter_type, filter);
    return schema_entry;
}

AggrFun
AIMSchemaBuilder::getAggrFun(const char *s_aggr_fun) const
{
    if (strcmp(s_aggr_fun,"min") == 0)
        return AggrFun::MIN;
    if (strcmp(s_aggr_fun,"max") == 0)
        return AggrFun::MAX;
    if (strcmp(s_aggr_fun,"sum") == 0)
        return AggrFun::SUM;
    assert(false);
    return AggrFun::MIN;
}

DataType
AIMSchemaBuilder::getDataType(const char *s_data_type) const
{
    if (strcmp(s_data_type,"int") == 0)
        return DataType::INT;
    if (strcmp(s_data_type,"uint") == 0)
        return DataType::UINT;
    if (strcmp(s_data_type,"double") == 0)
        return DataType::DOUBLE;
    if (strcmp(s_data_type,"ulong") == 0)
        return DataType::ULONG;
    assert(false);
    return DataType::INT;
}

Metric
AIMSchemaBuilder::getMetric(const char *s_metric) const
{
    if (strcmp(s_metric,"call") == 0 ||
        strcmp(s_metric,"local call") == 0 ||
        strcmp(s_metric,"non local call") == 0)
        return Metric::CALL;

    if (strcmp(s_metric,"duration") == 0 ||
        strcmp(s_metric,"local duration") == 0 ||
        strcmp(s_metric,"non local duration") == 0)
        return Metric::DUR;

    if (strcmp(s_metric,"cost") == 0 ||
        strcmp(s_metric,"local cost") == 0 ||
        strcmp(s_metric,"non local cost") == 0)
        return Metric::COST;
    assert(false);
    return Metric::CALL;
}

FilterType
AIMSchemaBuilder::getFilterType(const char *s_metric) const
{
    if (strcmp(s_metric,"call") == 0 ||
        strcmp(s_metric,"duration") == 0 ||
        strcmp(s_metric,"cost") == 0)
        return FilterType::NO;

    if (strcmp(s_metric,"local call") == 0 ||
        strcmp(s_metric,"local duration") == 0 ||
        strcmp(s_metric,"local cost") == 0)
        return FilterType::LOCAL;

    if (strcmp(s_metric,"non local call") == 0 ||
        strcmp(s_metric,"non local duration") == 0 ||
        strcmp(s_metric,"non local cost") == 0)
        return FilterType::NONLOCAL;
    assert(false);
    return FilterType::NO;
}

WindowType
AIMSchemaBuilder::getWindowType(const char *s_win_type) const
{
    if (strcmp(s_win_type,"tumb") == 0)
        return WindowType::TUMB;
    if (strcmp(s_win_type,"step") == 0)
        return WindowType::STEP;
    if (strcmp(s_win_type,"cont") == 0)
        return WindowType::CONT;
    assert(false);
    return WindowType::TUMB;
}

WindowLength
AIMSchemaBuilder::getWindowSize(const char *s_win_type) const
{
    if (strcmp(s_win_type,"w") == 0)
        return WindowLength::WEEK;
    if (strcmp(s_win_type,"d") == 0)
        return WindowLength::DAY;
    assert(false);
    return WindowLength::WEEK;
}

AIMSchemaBuilder::SE::FilterFPtr
AIMSchemaBuilder::getFilter(const char *s_metric) const
{
    if (strcmp(s_metric,"call") == 0 ||
        strcmp(s_metric,"duration") == 0 ||
        strcmp(s_metric,"cost") == 0) {
        return &noFilter;
    }

    if (strcmp(s_metric,"local call") == 0 ||
        strcmp(s_metric,"local duration") == 0 ||
        strcmp(s_metric,"local cost") == 0) {
        return &localFilter;
    }

    if (strcmp(s_metric,"non local call") == 0 ||
        strcmp(s_metric,"non local duration") == 0 ||
        strcmp(s_metric,"non local cost") == 0) {
        return &nonlocalFilter;
    }
    assert(false);
    return &noFilter;
}

/*
 * For all the aggregation funtions the metric is the same as the data type
 * of the aggregation. The first level has to do with the aggregation
 * function: min, max, sum. The second one is related to the metric: calls,
 * cost, duration. So a function for the update function pointer looks like:
 *
 * SE:UpdateFPtr update_f = updateMin<CostExtractor>
 *
 * for the "min of cost this week" attribute.
 */
void
AIMSchemaBuilder::setFunctions(const Value value, SE::InitDefFPtr *init_def_f,
                               SE::InitFPtr *init_f, SE::UpdateFPtr *upd_f,
                               SE::MaintainFPtr *maint_f) const
{
    switch (value.aggrFun()) {
    case AggrFun::MIN:
       setMinMetric(value.metric(), init_def_f, init_f, upd_f, maint_f);
       break;
    case AggrFun::MAX:
        setMaxMetric(value.metric(), init_def_f, init_f, upd_f, maint_f);
        break;
    case AggrFun::SUM:
        setSumMetric(value.metric(), init_def_f, init_f, upd_f, maint_f);
        break;
    default:
        assert(false);
        setMinMetric(value.metric(), init_def_f, init_f, upd_f, maint_f);
    }
}

/*
 * For the min aggregation function it sets the right metric.
 */
void
AIMSchemaBuilder::setMinMetric(const Metric metric, SE::InitDefFPtr *init_def_f,
                               SE::InitFPtr *init_f, SE::UpdateFPtr *upd_f,
                               SE::MaintainFPtr *maint_f) const
{
    switch (metric) {
    case Metric::CALL:
        *init_def_f = &initMinDef<CallExtractor>;
        *init_f = &initMin<CallExtractor>;
        *upd_f = &updateMin<CallExtractor>;
        *maint_f = &maintain<CallExtractor>;
        break;
    case Metric::COST:
        *init_def_f = &initMinDef<CostExtractor>;
        *init_f = &initMin<CostExtractor>;
        *upd_f = &updateMin<CostExtractor>;
        *maint_f = &maintain<CostExtractor>;
        break;
    case Metric::DUR:
        *init_def_f = &initMinDef<DurExtractor>;
        *init_f = &initMin<DurExtractor>;
        *upd_f = &updateMin<DurExtractor>;
        *maint_f = &maintain<DurExtractor>;
        break;
    default:
        assert(false);
        *init_def_f = &initMinDef<CallExtractor>;
        *init_f = &initMin<CallExtractor>;
        *upd_f = &updateMin<CallExtractor>;
        *maint_f = &maintain<CallExtractor>;
    }
}

/*
 * For the max aggregation function it sets the right metric.
 */
void
AIMSchemaBuilder::setMaxMetric(const Metric metric, SE::InitDefFPtr *init_def_f,
                               SE::InitFPtr *init_f, SE::UpdateFPtr *upd_f,
                               SE::MaintainFPtr *maint_f) const
{
    switch (metric) {
    case Metric::CALL:
        *init_def_f = &initMaxDef<CallExtractor>;
        *init_f = &initMax<CallExtractor>;
        *upd_f = &updateMax<CallExtractor>;
        *maint_f = &maintain<CallExtractor>;
        break;
    case Metric::COST:
        *init_def_f = &initMaxDef<CostExtractor>;
        *init_f = &initMax<CostExtractor>;
        *upd_f = &updateMax<CostExtractor>;
        *maint_f = &maintain<CostExtractor>;
        break;
    case Metric::DUR:
        *init_def_f = &initMaxDef<DurExtractor>;
        *init_f = &initMax<DurExtractor>;
        *upd_f = &updateMax<DurExtractor>;
        *maint_f = &maintain<DurExtractor>;
        break;
    default:
        assert(false);
        *init_def_f = &initMaxDef<CallExtractor>;
        *init_f = &initMax<CallExtractor>;
        *upd_f = &updateMax<CallExtractor>;
        *maint_f = &maintain<CallExtractor>;
    }
}

/*
 * For the sum aggregation function it sets the right metric.
 */
void
AIMSchemaBuilder::setSumMetric(const Metric metric, SE::InitDefFPtr *init_def_f,
                               SE::InitFPtr *init_f, SE::UpdateFPtr *upd_f,
                               SE::MaintainFPtr *maint_f) const
{
    switch (metric) {
    case Metric::CALL:
        *init_def_f = &initSumDef<CallExtractor>;
        *init_f = &initSum<CallExtractor>;
        *upd_f = &updateSum<CallExtractor>;
        *maint_f = &maintain<CallExtractor>;
        break;
    case Metric::COST:
        *init_def_f = &initSumDef<CostExtractor>;
        *init_f = &initSum<CostExtractor>;
        *upd_f = &updateSum<CostExtractor>;
        *maint_f = &maintain<CostExtractor>;
        break;
    case Metric::DUR:
        *init_def_f = &initSumDef<DurExtractor>;
        *init_f = &initSum<DurExtractor>;
        *upd_f = &updateSum<DurExtractor>;
        *maint_f = &maintain<DurExtractor>;
        break;
    default:
        assert(false);
        *init_def_f = &initSumDef<CallExtractor>;
        *init_f = &initSum<CallExtractor>;
        *upd_f = &updateSum<CallExtractor>;
        *maint_f = &maintain<CallExtractor>;
    }
}
