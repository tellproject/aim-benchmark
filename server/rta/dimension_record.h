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

#include <random>
#include <vector>

#include "rta/dimension_schema.h"

/*
 * This class implements a record of a subscriber in the
 * Dimensions(Cold data). A record is represented by a
 * vector of characters.
 */
class DimensionRecord
{
public:
    DimensionRecord() = default;
    ~DimensionRecord() = default;
    DimensionRecord(uint64_t subscriber_id, const DimensionSchema &schema);
    DimensionRecord(uint64_t subscriber_id, const DimensionSchema &schema, std::mt19937 &eng,
                    std::uniform_int_distribution<uint> &distr);

public:
    const std::vector<char>& data() const;

private:
    std::vector<char> _data;

private:
    void _fillIn(const DimensionSchema &schema,
                 std::vector<std::string> values, uint64_t subscriber_id);
};
