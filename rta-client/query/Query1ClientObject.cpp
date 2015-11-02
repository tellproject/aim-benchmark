#include "query/Query1ClientObject.h"

#include <cassert>

Query1ClientObject::Query1ClientObject() :
    AbstractQueryClientObject(),
    m_aggrCount(0),
    m_aggrSum(0)
{}

void
Query1ClientObject::merge(const char *buffer, size_t bufferSize)
{
    assert(bufferSize == sizeof(uint64_t) + sizeof(uint32_t));
    m_aggrSum += *(reinterpret_cast<uint64_t *>(const_cast<char *>(buffer)));
    m_aggrCount += *(reinterpret_cast<uint32_t *>
                     (const_cast<char *>(&buffer[sizeof(uint64_t)])));
}

std::vector<char>
Query1ClientObject::getResult()
{
    std::vector<char> result(sizeof(double), '0');
    *(reinterpret_cast<double *>(result.data())) = ((double) m_aggrSum) /
            ((double) m_aggrCount);
    return std::move(result);
}
