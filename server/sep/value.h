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

#include "server/sep/utils.h"

#include <crossbow/string.hpp>

/*
 * Describes the value of an AM attribute. An attribute is an aggregation over
 * a metric. We keep the aggregation function (min, max, sum), the metric
 * (call, cost, duration), the type descriptor of the metric (the actual data
 * type: int, uint, long, ulong, double and it's size) and the type descriptor
 * of the result of the aggregation function. The two types are always the same
 * exept for <duration, sum> where the aggregation type is 64-bit while the
 * metric is 32-bit.
 *
 * E.g. "duration this week", then:
 *      aggregation function = sum,
 *      metric = duration,
 *      metric type descriptor = uint, size = 4bytes,
 *      aggregation type descriptor = ulong, size = 8 bytes.
 */
class Value
{
public:
    Value(AggrFun aggr_type, tell::store::FieldType field_type, Metric metric, tell::store::FieldType metric_type, const crossbow::string &name);

public:
    /*
     * Simple getters.
     */
    AggrFun aggrFun() const { return _aggr_fun; }
    Metric metric() const { return _metric; }
    tell::store::FieldType type() const { return _aggr_field.type(); }
    uint8_t dataSize() const { return _aggr_field.staticSize(); }
    const crossbow::string &name() const {return _aggr_field.name(); }
    tell::store::FieldType metricType() const { return _metric_field_base.type(); }
    uint8_t metricSize() const {return _metric_field_base.staticSize(); }

private:
    AggrFun _aggr_fun;
    tell::store::Field _aggr_field;
    Metric _metric;
    tell::store::FieldBase _metric_field_base;
};
