#pragma once

#include <array>

#include "query/AbstractQueryServerObject.h"
#include "rta/dim_column_finder.h"

/*
 * This class implements the logic for the Longest Call Query
 * on the server side. Query 6.2.3:
 *
 * The motivation for this query is to find the subscriber with
 * the longest call in this day and this week for local and
 * long distance calls. We are interested in subscribers
 * residing in a specific country.
 */
class Query6ServerObject: public AbstractQueryServerObject
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
    Query6ServerObject(uint32_t query_id, uint8_t threads_num,
                       AbstractRTACommunication* communication,
                       std::vector<char> additional_args,
                       const AIMSchema &aim_schema,
                       const DimensionSchema& dim_schema,
                       uint16_t country_id);

    ~Query6ServerObject() = default;

public:
    void processBucket(uint thread_id, const BucketReference&) override;
    void processLastBucket(uint thread_id, const BucketReference&) override;
    std::pair<std::vector<char>, Status> popResult() override;

private:
    const uint16_t _country_id;
    AimColumnFinder<Metric::DUR, AggrFun::MAX, FilterType::LOCAL, WindowLength::DAY> _dur_max_local_day;
    AimColumnFinder<Metric::DUR, AggrFun::MAX, FilterType::LOCAL, WindowLength::WEEK> _dur_max_local_week;
    AimColumnFinder<Metric::DUR, AggrFun::MAX, FilterType::NONLOCAL, WindowLength::DAY> _dur_max_long_day;
    AimColumnFinder<Metric::DUR, AggrFun::MAX, FilterType::NONLOCAL, WindowLength::WEEK> _dur_max_long_week;
    DimColumnFinder<DimensionAttribute::REGION_COUNTRY> _region_country;
    DimColumnFinder<DimensionAttribute::SUBSCRIBER_ID> _subscriber_id;
    std::array<Query6Result, RTA_THREAD_NUM> _entries;
};
