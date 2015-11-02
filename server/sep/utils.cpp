#include "utils.h"

#include <cassert>

#include "util/system-constants.h"

std::string
stringDataType(DataType type)
{
    switch (type) {
    case DataType::INT:
        return "int";
    case DataType::UINT:
        return "uint";
    case DataType::ULONG:
        return "ulong";
    case DataType::DOUBLE:
        return "double";
    default:
        return "data_type error";
    }
}

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

uint32_t
belongToThisServer(ulong sub_id, uint server_id)
{
    return ((sub_id >> 8) % SERVER_NUM) == server_id;
}

uint
findAvailableCores()
{
    return std::thread::hardware_concurrency();
}
