#pragma once

#include <array>

#include "query/AbstractQueryServerObject.h"

/*
 * Q1: SELECT avg(total_duration_this_week)
 * FROM WT
 * WHERE number_of_local_calls_this_week > alpha;
 */
class Query1ServerObject: public AbstractQueryServerObject
{
public:
    Query1ServerObject(uint32_t query_id, uint8_t threads_num,
                       AbstractRTACommunication *communication,
                       std::vector<char> additional_args,
                       const AIMSchema& aim_schema, uint32_t alpha);

    ~Query1ServerObject() = default;

public:
    void processBucket(uint thread_id, const BucketReference&) override;
    void processLastBucket(uint thread_id, const BucketReference&) override;

    /**
     * @brief popResult
     * @return  uint64_t SUM | uint32_t COUNT
     */
    std::pair<std::vector<char>, Status> popResult() override;

private:
    const uint32_t _alpha;
    AimColumnFinder<Metric::CALL, AggrFun::SUM, FilterType::LOCAL, WindowLength::WEEK> _calls_sum_local_week;
    AimColumnFinder<Metric::DUR, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _dur_sum_all_week;
    std::array<uint64_t, RTA_THREAD_NUM> _sums;
    std::array<uint32_t, RTA_THREAD_NUM> _counts;
};
