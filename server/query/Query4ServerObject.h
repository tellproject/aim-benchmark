#pragma once

#include <array>

#include "query/AbstractQueryServerObject.h"
#include "rta/dim_column_finder.h"

//TODO: avg(number of local calls this week) ???
/*
 *
 * 6.2.1 Call-intensive Cities Query
 * The motivation for this query is to find out the cities in which people
 * talk much with each other, i.e. where average number of local calls and
 * total duration of local calls is high. More concretely, this means to
 * compute for each city the average number of local calls and the total
 * duration of local calls in the current week for the most valuable subscribers,
 * i.e. subscribers whose average number of local calls and the total duration of
 * local calls in the current week exceed certain thresholds. This is a query that
 * can be run periodically by the marketing department in order to see on which
 * cities they should focus with marketing campaigns that involve local calls.
 *
 * SQL Statement
 * SELECT city, avg(number of local calls this week), sum(total duration of local calls this week)
 * FROM VWT, RegionInfo
 * WHERE number of local calls this week > numberThreshold
 * AND total duration of local calls this week > durationThreshold
 * AND VWT.zip = RegionInfo.zip
 * GROUP BY city
 */
class Query4ServerObject: public AbstractQueryServerObject
{
public:
    struct CityEntry
    {
        CityEntry(): avg_sum(0), avg_cnt(0), sum(0)
        {}

        CityEntry(uint64_t avg_sum, uint32_t avg_cnt, uint64_t sum):
            avg_sum(avg_sum),
            avg_cnt(avg_cnt),
            sum(sum)
        {}

        std::atomic<uint64_t> avg_sum;
        std::atomic<uint32_t> avg_cnt;
        std::atomic<uint64_t> sum;
    };

public:
    Query4ServerObject(uint32_t query_id, uint8_t threads_num,
                       AbstractRTACommunication* communication,
                       std::vector<char> additional_args,
                       const AIMSchema &aim_schema,
                       const DimensionSchema &dim_schema,
                       uint32_t alpha, uint32_t beta);

    ~Query4ServerObject() = default;

public:
    void processBucket(uint thread_id, const BucketReference&) override;
    void processLastBucket(uint thread_id, const BucketReference&) override;
    std::pair<std::vector<char>, Status> popResult() override;

private:
    const uint32_t _alpha;
    const uint32_t _beta;
    AimColumnFinder<Metric::CALL, AggrFun::SUM, FilterType::LOCAL, WindowLength::WEEK> _calls_sum_local_week;
    AimColumnFinder<Metric::DUR, AggrFun::SUM, FilterType::LOCAL, WindowLength::WEEK> _dur_sum_local_week;
    DimColumnFinder<DimensionAttribute::REGION_CITY> _region_city;
    std::vector<CityEntry> _entries;
};

