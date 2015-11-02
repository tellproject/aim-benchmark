#pragma once

#include "query/AbstractQueryServerObject.h"

#include <array>

#include "rta/dim_column_finder.h"

/*
 * This class implements the logic for the Highest Revenue query
 * on the server side. Query 6.2.2:
 *
 * SELECT region, sum(total cost of local calls this week) as local,
 *        sum(total cost of long distance calls) as long_distance
 * FROM VWT, SubscriptionType t, SubscriberCategory c, RegionInfo r
 * WHERE t.type = subscriptionType AND c.type = subscriberCategory AND
 *       VWT.subscription-type = t.id AND VWT.category = c.id AND
 *       VWT.zip = r.zip
 * GROUP BY region
 */
class Query5ServerObject: public AbstractQueryServerObject
{
public:
    struct RegionEntry
    {
        double local_sum = 0;
        double long_sum = 0;
    };

public:
    Query5ServerObject(uint32_t query_id, uint8_t threads_num,
                       AbstractRTACommunication* communication,
                       std::vector<char> additional_args,
                       const AIMSchema &aim_schema,
                       const DimensionSchema& dim_schema,
                       uint16_t type, uint16_t category);

    ~Query5ServerObject() = default;

public:
    void processBucket(uint thread_id, const BucketReference&) override;
    void processLastBucket(uint thread_id, const BucketReference&) override;
    std::pair<std::vector<char>, Status> popResult() override;

private:
    const uint16_t _type;
    const uint16_t _category;
    AimColumnFinder<Metric::COST, AggrFun::SUM, FilterType::LOCAL, WindowLength::WEEK> _cost_sum_local_week;
    AimColumnFinder<Metric::COST, AggrFun::SUM, FilterType::NONLOCAL, WindowLength::WEEK> _cost_sum_long_week;
    DimColumnFinder<DimensionAttribute::REGION_REGION> _region_region;
    DimColumnFinder<DimensionAttribute::SUBSCRIPTION_TYPE> _subscription_type;
    DimColumnFinder<DimensionAttribute::SUBSCRIBER_CATEGORY_TYPE> _subscriber_category;

    std::array<std::vector<RegionEntry>, RTA_THREAD_NUM> _entries;
};
