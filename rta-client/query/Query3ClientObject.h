#pragma once

#include "query/AbstractQueryClientObject.h"
#include "util/sparsehash/dense_hash_map"

class Query3ClientObject : public AbstractQueryClientObject
{
public:
    Query3ClientObject();
    ~Query3ClientObject() = default;

    struct Q3Tuple
    {
        Q3Tuple& operator +=(const Q3Tuple& other) {
            cost_sum_all_week += other.cost_sum_all_week;
            dur_sum_all_week += other.dur_sum_all_week;
            call_count = other.call_count;
            return *this;
        }

        double cost_sum_all_week = 0.;
        uint32_t dur_sum_all_week = 0.;
        uint32_t call_count;
    };

    struct Q3Result
    {
        Q3Result() {
            outliers.set_empty_key(-2);
            outliers.set_deleted_key(-1);
        }
        static constexpr uint32_t SMALL_THRESHOLD = 100;
        Q3Tuple small_values[SMALL_THRESHOLD];
        google::dense_hash_map<int64_t, Q3Tuple> outliers;
    };

public:
    void merge(const char *buffer, size_t bufferSize) override;

    std::vector<char> getResult() override;

private:
    Q3Result _result;
    uint64_t m_totalSize;
    std::vector<std::vector<char>> m_resultBuffers;
};
