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

#include "server/sep/type_descriptor.h"
#include "server/sep/utils.h"

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
