#include "query/Query3ClientObject.h"

#include <cstdio>
#include <cstring>

Query3ClientObject::Query3ClientObject() :
    AbstractQueryClientObject(),
    m_totalSize(0),
    m_resultBuffers(std::vector<std::vector<char>>())
{}

void
Query3ClientObject::merge(const char *buffer, size_t bufferSize)
{
    char* tmp = const_cast<char *>(buffer);
    auto number_of_result_tuples = *reinterpret_cast<const size_t*>(tmp);
    tmp += sizeof(number_of_result_tuples);
    for (uint i=0; i<number_of_result_tuples; ++i) {
        Q3Tuple& new_tuple = *reinterpret_cast<Q3Tuple*>(tmp);
        tmp += sizeof(Q3Tuple);
        if (new_tuple.call_count < Q3Result::SMALL_THRESHOLD) {
            _result.small_values[new_tuple.call_count] += new_tuple;
        }
        else {
            _result.outliers[new_tuple.call_count] += new_tuple;
        }
    }
    assert(tmp == buffer + bufferSize);
}

std::vector<char>
Query3ClientObject::getResult()
{
    return std::vector<char>();
}
