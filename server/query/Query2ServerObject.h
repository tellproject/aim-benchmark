#pragma once

#include <array>

#include "query/AbstractQueryServerObject.h"

/*
 * Q2: SELECT max(most_expensive_call_this_week)
 * FROM WT
 * WHERE number_of_calls_this_week > alpha;
 */
class Query2ServerObject : public AbstractQueryServerObject
{
public:
    Query2ServerObject(uint32_t query_id, uint8_t threads_num,
                       AbstractRTACommunication* communication,
                       std::vector<char> additional_args,
                       const AIMSchema &aim_schema, uint32_t alpha);

     ~Query2ServerObject() = default;

public:
    void processBucket(uint thread_id, const BucketReference&) override;
    void processLastBucket(uint thread_id, const BucketReference&) override;

    /**
     * @brief popResult
     * @return double MAX
     */
    std::pair<std::vector<char>, Status> popResult() override;

private:
    AimColumnFinder<Metric::COST, AggrFun::MAX, FilterType::NO, WindowLength::WEEK> _cost_max_all_week;
    AimColumnFinder<Metric::CALL, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _calls_sum_all_week;
    uint32_t _alpha;

    std::array<double, RTA_THREAD_NUM> _maxs;
};
