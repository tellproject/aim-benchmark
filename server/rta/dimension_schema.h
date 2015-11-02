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

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <vector>

#include "rta/utils.h"

/*
 * This class implements the schema of the dimensions columns for a
 * subscriber. Every subscriber has a number of columns each of which
 * correspond to a dimension column.
 */
class DimensionSchema
{
public:
    DimensionSchema();
    ~DimensionSchema() = default;

public:
    const std::vector<uint16_t> &sizes() const;
    std::size_t size() const;
    std::size_t sizeAt(std::size_t pos) const;
    std::size_t numOfEntries() const;
    std::size_t getOffset(DimensionAttribute attribute) const;

private:
    std::vector<uint16_t> _offsets;
    std::vector<uint16_t> _sizes;
    std::size_t _size;
};
