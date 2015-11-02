#include <cassert>
#include "dimension_schema.h"

DimensionSchema::DimensionSchema(): _size(0)
{
    _offsets.push_back(0);
    for (uint16_t i=0; i<int(DimensionAttribute::count); ++i) {
        auto current_size = dimension_attribute_size(DimensionAttribute(i));
        _sizes.push_back(current_size);
        _size += current_size;
        _offsets.push_back(_size);
    }
}

const std::vector<uint16_t>&
DimensionSchema::sizes() const
{
    return _sizes;
}

std::size_t
DimensionSchema::size() const
{
    return _size;
}

std::size_t
DimensionSchema::sizeAt(size_t pos) const
{
    return _sizes[pos];
}

std::size_t
DimensionSchema::numOfEntries() const
{
    return _sizes.size();
}

std::size_t DimensionSchema::getOffset(DimensionAttribute attribute) const
{
    return _offsets[size_t(attribute)];
}
