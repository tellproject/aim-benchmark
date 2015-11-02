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

#include "aim_schema.h"
#include "column-map/ColumnMap.h"
#include "util/system-constants.h"

template<Metric METRIC, AggrFun AGGR, FilterType FILTER_TYPE, WindowLength WINDOW_SIZE>
struct AimColumnFinder
{
    AimColumnFinder() = default;
    AimColumnFinder(const AIMSchema& schema)
    {
        _offset = schema.getOffset(METRIC, AGGR, FILTER_TYPE, WINDOW_SIZE);
        assert(_offset >= 0);
        _offset *= RECORDS_PER_BUCKET;
    }

    template <Metric FAKE_METRIC = METRIC>
    typename std::enable_if<FAKE_METRIC == Metric::CALL, const uint32_t*>::type get(BucketReference bucket_ref)
    {
        static_assert(FAKE_METRIC == METRIC, "stop screwing around");
        assert(_offset != -1);
        return reinterpret_cast<const uint32_t*>(bucket_ref.bucket_start + _offset);
    }

    template <Metric FAKE_METRIC = METRIC>
    typename std::enable_if<FAKE_METRIC == Metric::COST, const double*>::type get(BucketReference bucket_ref)
    {
        static_assert(FAKE_METRIC == METRIC, "stop screwing around");
        assert(_offset != -1);
        return reinterpret_cast<const double*>(bucket_ref.bucket_start + _offset);
    }

    template <Metric FAKE_METRIC = METRIC>
    typename std::enable_if<FAKE_METRIC == Metric::DUR, const uint32_t*>::type get(BucketReference bucket_ref)
    {
        static_assert(FAKE_METRIC == METRIC, "stop screwing around");
        assert(_offset != -1);
        return reinterpret_cast<const uint32_t*>(bucket_ref.bucket_start + _offset);
    }

    int32_t getOffset()
    {
        assert(_offset != -1);
        return _offset / RECORDS_PER_BUCKET;
    }

private:
    int64_t _offset = -1;
};
