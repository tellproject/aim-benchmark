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

#include "column-map/ColumnMap.h"
#include "rta/dimension_schema.h"
#include "sep/aim_schema.h"

template<DimensionAttribute ATTR>
struct DimColumnFinder
{
    DimColumnFinder() = default;
    DimColumnFinder(const AIMSchema& aim_schema, const DimensionSchema& dim_schema)
    {
        _offset = aim_schema.size() + dim_schema.getOffset(ATTR);
        _offset *= RECORDS_PER_BUCKET;
    }

    template <DimensionAttribute FAKE_ATTR = ATTR>
    typename std::enable_if<FAKE_ATTR == DimensionAttribute::SUBSCRIBER_ID, const uint64_t*>::type
    get(BucketReference bucket_ref)
    {
        static_assert(FAKE_ATTR == ATTR, "stop screwing around");
        assert(_offset != -1);
        return reinterpret_cast<const uint64_t*>(bucket_ref.bucket_start + _offset);
    }
    template <DimensionAttribute FAKE_ATTR = ATTR>
    typename std::enable_if<FAKE_ATTR != DimensionAttribute::SUBSCRIBER_ID, const uint16_t*>::type
    get(BucketReference bucket_ref)
    {
        static_assert(FAKE_ATTR == ATTR, "stop screwing around");
        assert(_offset != -1);
        return reinterpret_cast<const uint16_t*>(bucket_ref.bucket_start + _offset);
    }

    int32_t getOffset()
    {
        assert(_offset != -1);
        return _offset / RECORDS_PER_BUCKET;
    }

private:
    int64_t _offset = -1;
};
