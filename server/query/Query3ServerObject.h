#pragma once

#include <array>
#include <cstdio>
#include <cstring>
#include <mutex>

#include "util/system-constants.h"
#include "query/AbstractQueryServerObject.h"

/*
 * Q3: SELECT sum(total_cost_this_week)/sum(total_duration_this_week) as cost_ratio
 * FROM WT
 * GROUP BY number_of_calls_this_week
 */
class Query3ServerObject : public AbstractQueryServerObject
{
public:
    Query3ServerObject(uint32_t query_id, uint8_t threads_num,
                       AbstractRTACommunication* communication,
                       std::vector<char> additional_args,
                       const AIMSchema &aim_schema);

    ~Query3ServerObject() = default;

public:
    void processBucket(uint thread_id, const BucketReference&) override;
    void processLastBucket(uint thread_id, const BucketReference&) override;
    std::pair<std::vector<char>, Status> popResult() override;

private:

    AimColumnFinder<Metric::CALL, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _calls_sum_all_week;
    AimColumnFinder<Metric::COST, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _cost_sum_all_week;
    AimColumnFinder<Metric::DUR, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _dur_sum_all_week;
    std::array<Q3Result, RTA_THREAD_NUM> _active_results;
    Status _status = Status::HAS_NEXT;
};
