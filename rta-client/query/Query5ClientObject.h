#pragma once

#include <string>
#include <unordered_map>

#include "query/AbstractQueryClientObject.h"

/*
 * This class implements the logic for the Highest Revenue query
 * on the client side. Query 6.2.2:
 *
 * SELECT region, sum(total cost of local calls this week) as local,
 *        sum(total cost of long distance calls) as long_distance
 * FROM VWT, SubscriptionType t, SubscriberCategory c, RegionInfo r
 * WHERE t.type = subscriptionType AND c.type = subscriberCategory AND
 *       VWT.subscription-type = t.id AND VWT.category = c.id AND
 *       VWT.zip = r.zip
 * GROUP BY region
 */
class Query5ClientObject : public AbstractQueryClientObject
{
public:
    struct RegionEntry
    {
        double local_sum = 0;
        double long_sum = 0;
    };
public:
    Query5ClientObject();
    ~Query5ClientObject() = default;

public:
    void merge(const char *buffer, size_t bufferSize) override;
    std::vector<char> getResult() override;

private:
    std::unordered_map<std::string, RegionEntry> _entries;
};

