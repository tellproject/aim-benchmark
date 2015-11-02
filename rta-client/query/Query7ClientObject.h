#pragma once

#include "query/AbstractQueryClientObject.h"

#include <limits>

class Query7ClientObject : public AbstractQueryClientObject
{
public:
    struct Query7Result
    {
        uint64_t subscriber_id = 0;
        double avg_money_per_time = std::numeric_limits<double>::max();
    };

public:
    ~Query7ClientObject() = default;

public:
    void merge(const char *buffer, size_t bufferSize) override;
    std::vector<char> getResult() override;

private:
    Query7Result _result;
};

