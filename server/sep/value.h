#pragma once

#include "sep/type_descriptor.h"
#include "sep/utils.h"

/*
 * Describes the value of an AM attribute. An attribute is an aggregation over
 * a metric. We keep the aggregation function (min, max, sum), the metric
 * (call, cost, duration), the type descriptor of the metric (the actual data
 * type: int, uint, long, ulong, double and it's size) and the type descriptor
 * of the result of the aggregation function.
 *
 * E.g. "duration this week", then:
 *      aggregation function = sum,
 *      metric = duration,
 *      metric type descriptor = uint, size = 4bytes,
 *      aggregation type descriptor = uint, size = 4 bytes.
 */
class Value
{
public:
    Value(AggrFun aggr_type, DataType aggr_data_type, Metric metric,
          DataType event_attr_data_type);

public:
    /*
     * Simple getters.
     */
    AggrFun aggrFun() const { return _aggr_fun; }
    Metric metric() const { return _metric; }
    uint8_t aggrDataSize() const { return _aggr_type_desc.size(); }
    uint8_t metricDataSize() const { return _metric_desc.size(); }

    //lb: added for generic benchmarking framework
    DataType type() const { return _aggr_type_desc.type(); }

private:
    AggrFun _aggr_fun;
    TypeDescriptor _aggr_type_desc;
    Metric _metric;
    TypeDescriptor _metric_desc;
};
