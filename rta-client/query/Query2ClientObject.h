#pragma once

#include "query/AbstractQueryClientObject.h"

class Query2ClientObject : public AbstractQueryClientObject
{
public:
    Query2ClientObject();
    ~Query2ClientObject() = default;

public:
    void merge(const char *buffer, size_t bufferSize) override;

    /**
     * @brief getResult
     * @return double MAX
     */
    std::vector<char> getResult() override;

private:
    double m_aggrMax;

};
