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
#include "utils.h"

#include <cassert>

//std::string
//stringDataType(DataType type)
//{
//    switch (type) {
//    case DataType::INT:
//        return "int";
//    case DataType::UINT:
//        return "uint";
//    case DataType::ULONG:
//        return "ulong";
//    case DataType::DOUBLE:
//        return "double";
//    default:
//        return "data_type error";
//    }
//}

std::string
stringAggrFun(AggrFun fun)
{
    switch (fun) {
    case AggrFun::MIN:
        return "min";
    case AggrFun::MAX:
        return "max";
    case AggrFun::SUM:
        return "sum";
    default:
        assert(false);
        return "min";
    }
}

std::string
stringFilter(FilterType type)
{
    switch (type) {
    case FilterType::NO:
        return "no filter";
    case FilterType::LOCAL:
        return "local";
    case FilterType::NONLOCAL:
        return "non local";
    default:
        assert(false);
        return "no filter";
    }
}

std::string
stringMetric(Metric metric)
{
    switch (metric) {
    case Metric::CALL:
        return "call";
    case Metric::DUR:
        return "dur";
    case Metric::COST:
        return "cost";
    default:
        assert(false);
        return "call";
    }
}

std::string
stringWindowType(WindowType type)
{
    switch (type) {
    case WindowType::TUMB:
        return "tumb";
    case WindowType::STEP:
        return "step";
    case WindowType::CONT:
        return "cont";
    default:
        assert(false);
        return "tumb";
    }
}

std::string
stringWindowSize(ulong size)
{
    switch (size) {
    case MSECS_PER_DAY:
        return "day";
    case MSECS_PER_WEEK:
        return "week";
    default:
        assert(false);
        return "week";
    }
}
