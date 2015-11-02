#include "query/Query2ClientObject.h"

#include <cassert>

Query2ClientObject::Query2ClientObject() :
    AbstractQueryClientObject(),
    m_aggrMax(0)
{}

void
Query2ClientObject::merge(const char *buffer, size_t bufferSize)
{
    assert(bufferSize == sizeof(double));
    double mostExpensiveCallWeek = *reinterpret_cast<double *>(const_cast<char *>(buffer));
    if (mostExpensiveCallWeek > m_aggrMax) {
        m_aggrMax = mostExpensiveCallWeek;
    }
}

std::vector<char>
Query2ClientObject::getResult()
{
    std::vector<char> result(sizeof(double));
    *(reinterpret_cast<double *>(result.data())) = m_aggrMax;
    return std::move(result);
}
