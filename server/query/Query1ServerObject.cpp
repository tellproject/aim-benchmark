#include "Query1ServerObject.h"

#include "column-map/ColumnMap.h"

Query1ServerObject::Query1ServerObject(uint32_t query_id, uint8_t threads_num,
                                       AbstractRTACommunication* communication,
                                       std::vector<char> additional_args,
                                       const AIMSchema &aim_schema,
                                       uint32_t alpha):

    AbstractQueryServerObject(query_id, threads_num, communication,
                              std::move(additional_args)),
    _alpha(alpha),
    _calls_sum_local_week(aim_schema),
    _dur_sum_all_week(aim_schema)
{
    for (auto& val : _sums) {
        val = 0;
    }
    for (auto& val : _counts) {
        val = 0;
    }
}

void
Query1ServerObject::processBucket(uint thread_id, const BucketReference &bucket)
{
    assert(bucket.num_of_records == RECORDS_PER_BUCKET);
    auto avg = q1_simple<RECORDS_PER_BUCKET>(_calls_sum_local_week.get(bucket), _alpha,
                                             _dur_sum_all_week.get(bucket));
    _sums[thread_id] += avg.sum;
    _counts[thread_id] += avg.count;
}

void
Query1ServerObject::processLastBucket(uint thread_id, const BucketReference &bucket)
{
    auto avg = q1_simple(_calls_sum_local_week.get(bucket), _alpha,
                         _dur_sum_all_week.get(bucket),
                         bucket.num_of_records);

    _sums[thread_id] += avg.sum;
    _counts[thread_id] += avg.count;
}

auto
Query1ServerObject::popResult() -> std::pair<std::vector<char>, Status>
{
    uint64_t sum = std::accumulate(_sums.begin(), _sums.end(), uint64_t(0));
    uint32_t count = std::accumulate(_counts.begin(), _counts.end(), uint32_t(0));
    std::vector<char> resultBuf(sizeof(sum) + sizeof(count));
    *(reinterpret_cast<decltype(sum) *>(resultBuf.data())) = sum;
    *(reinterpret_cast<decltype(count) *>(&resultBuf[sizeof(sum)])) = count;
    return std::make_pair(std::move(resultBuf), Status::DONE);
}
