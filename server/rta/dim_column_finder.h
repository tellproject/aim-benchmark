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
