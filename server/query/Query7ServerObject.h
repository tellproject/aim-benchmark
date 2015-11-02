#pragma once

#include <array>

#include "query/AbstractQueryServerObject.h"
#include "rta/dim_column_finder.h"

/*
 * This class implements the logic for the Flat Rate Subscribers query
 * on the server side. Query 6.2.4:
 *
 * SELECT subscriber-id, name, address,
 * min(avg cost of calls this T / avg duration of calls this T) as flat-rate
 * FROM VWT, SubscriberValue v
 * WHERE v.type = subscriberValueType
 * AND VWT.value = v.id
 */
class Query7ServerObject: public AbstractQueryServerObject
{
public:
    struct Query7Result
    {
        uint64_t subscriber_id = 0;
        double avg_money_per_time = std::numeric_limits<double>::max();
    };

public:
    Query7ServerObject(uint32_t queryNumber,
                       uint8_t numberOfThreads,
                       AbstractRTACommunication *communicationObject,
                       std::vector<char> additionalArguments,
                       const AIMSchema &aim_schema,
                       const DimensionSchema& dim_schema,
                       WindowLength window_length,
                       uint16_t subscriber_value_type);

    ~Query7ServerObject() = default;

public:
    void processBucket(uint thread_id, const BucketReference&) override;
    void processLastBucket(uint thread_id, const BucketReference&) override;
    std::pair<std::vector<char>, Status> popResult() override;

private:
    const uint16_t _value_type_id;
    const WindowLength _window_length;
    AimColumnFinder<Metric::COST, AggrFun::SUM, FilterType::NO, WindowLength::DAY> _cost_sum_all_day;
    AimColumnFinder<Metric::COST, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _cost_sum_all_week;
    AimColumnFinder<Metric::DUR, AggrFun::SUM, FilterType::NO, WindowLength::DAY> _dur_sum_all_day;
    AimColumnFinder<Metric::DUR, AggrFun::SUM, FilterType::NO, WindowLength::WEEK> _dur_sum_all_week;
    DimColumnFinder<DimensionAttribute::SUBSCRIBER_VALUE_TYPE> _subscriber_value_type;
    DimColumnFinder<DimensionAttribute::SUBSCRIBER_ID> _subscriber_id;
    std::array<Query7Result, RTA_THREAD_NUM> _entries;
};
