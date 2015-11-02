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
