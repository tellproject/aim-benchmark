#include "Query4ServerObject.h"

#include "util/dimension-tables-unique-values.h"

Query4ServerObject::Query4ServerObject(uint32_t query_id,
                                       uint8_t threads_num,
                                       AbstractRTACommunication *communication,
                                       std::vector<char> additional_args,
                                       const AIMSchema &aim_schema,
                                       const DimensionSchema& dim_schema,
                                       uint32_t alpha, uint32_t beta):

    AbstractQueryServerObject(query_id, threads_num, communication,
                              std::move(additional_args)),
    _alpha(alpha),
    _beta(beta),
    _calls_sum_local_week(aim_schema),
    _dur_sum_local_week(aim_schema),
    _region_city(aim_schema, dim_schema),
    _entries(region_unique_city.size())
{}

void
Query4ServerObject::processBucket(uint thread_id, const BucketReference& bucket)
{
    assert(bucket.num_of_records == RECORDS_PER_BUCKET);
    //q4_simple creates a Q4Entry (avg_sum, avg_cnt, sum) for every city
    q4_simple<RECORDS_PER_BUCKET>(_calls_sum_local_week.get(bucket), _alpha,
                                  _dur_sum_local_week.get(bucket), _beta,
                                  _region_city.get(bucket), _entries);
}

void
Query4ServerObject::processLastBucket(uint thread_id, const BucketReference& bucket)
{
    q4_simple(_calls_sum_local_week.get(bucket), _alpha,
              _dur_sum_local_week.get(bucket), _beta,
              _region_city.get(bucket), bucket.num_of_records, _entries);
}

auto
Query4ServerObject::popResult() -> std::pair<std::vector<char>, Status>
{
    size_t res_size = region_unique_city.size() * (sizeof(size_t) + sizeof(CityEntry));
    for (std::string &city: region_unique_city) {
        res_size += city.size();
    }
    std::vector<char> res(res_size);
    char *tmp = res.data();
    for (size_t i=0; i<_entries.size(); ++i) {

        CityEntry &entry = _entries[i];
        serialize(region_unique_city[i], tmp);
        memcpy(tmp, &entry, sizeof(CityEntry));
        tmp += sizeof(CityEntry);
    }
    assert(tmp == res.data() + res.size());
    return std::make_pair(std::move(res), Status::DONE);
}


