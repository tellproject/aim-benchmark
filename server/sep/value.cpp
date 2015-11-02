#include "value.h"

#include <cassert>

Value::Value(AggrFun aggr_fun, DataType aggr_data_type, Metric metric,
             DataType metric_data_type):

    _aggr_fun(aggr_fun),
    _aggr_type_desc(aggr_data_type),
    _metric(metric),
    _metric_desc(metric_data_type)
{
    assert(_aggr_type_desc.size() == _metric_desc.size());
}
