#pragma once

#include "query/AbstractQueryClientObject.h"

class Query1ClientObject : public AbstractQueryClientObject
{
public:
    Query1ClientObject();
    ~Query1ClientObject() = default;

public:
    void merge(const char *buffer, size_t bufferSize) override;

    /**
     * @brief getResult
     * @return double AVG
     */
    std::vector<char> getResult() override;

private:
    uint32_t m_aggrCount;
    uint64_t m_aggrSum;

};
