#pragma once

#include "query/AbstractQueryClientObject.h"

class Query6ClientObject : public AbstractQueryClientObject
{
public:
    struct Query6Result
    {
        uint64_t max_local_week_id = 0;
        uint32_t max_local_week = 0;
        uint64_t max_local_day_id = 0;
        uint32_t max_local_day = 0;
        uint64_t max_long_week_id = 0;
        uint32_t max_long_week = 0;
        uint64_t max_long_day_id = 0;
        uint32_t max_long_day = 0;
    };
public:
    ~Query6ClientObject() = default;

public:
    void merge(const char *buffer, size_t bufferSize) override;
    std::vector<char> getResult() override;

private:
    Query6Result _result;
};

