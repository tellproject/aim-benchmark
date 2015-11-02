/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
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
