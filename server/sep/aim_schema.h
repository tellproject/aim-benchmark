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

#include <cassert>
#include <iostream>
#include <vector>
#include <unordered_map>

#include "server/sep/aim_schema_entry.h"

/*
 * This class contains infomation about every atttibute in the AM. Every attribute
 * in the AM is represented by a schema entry. We store:
 *
 * _entries = the schema entries of all the AM attributes.
 *
 * _offsets = we keep the offset of each attribute within the record. Since the
 *            record size is constant offsets do not change. This information
 *            assists us in moving swiftly within the record. We use _offsets
 *            during the campaign evaluation procedure.
 *
 * _size    = size of an AM record
 *
 * The size of the timestamp of the subscriber's last event is added to both
 * _f_size and _c_size.
 */

class AIMSchema
{
public:
    AIMSchema();
    AIMSchema(const AIMSchema &other) = default;
    AIMSchema& operator=(AIMSchema &other) = delete;
    AIMSchema(AIMSchema &&other);
    AIMSchema& operator=(AIMSchema &&other);

public:
    void addEntry(const AIMSchemaEntry&);
    const AIMSchemaEntry& operator[](size_t pos) const { return _entries[pos]; }
    std::size_t numOfEntries() const { return _entries.size(); }
    size_t size() const { return _size; }
    uint16_t OffsetAt(uint pos) const { return _offsets[pos]; }
    std::vector<uint16_t> sizes() const;

    uint64_t getOffset(Metric metric, AggrFun aggr_fun, FilterType filter_type,
                       WindowLength window_size) const;
    friend std::ostream& operator<<(std::ostream& out, const AIMSchema &schema);

    //lb: added for generic benchmarking framework
    DataType typeAt(uint pos) const { return _entries[pos].type(); }

private:
    uint64_t _getEntryHash(Metric metric, AggrFun aggr_fun,
                           FilterType filter_type,
                           WindowLength window_len) const;
private:
    std::vector<AIMSchemaEntry> _entries;
    std::vector<uint16_t> _offsets;
    size_t _size;
    std::unordered_map<uint64_t, uint64_t> _entry_to_offset;
};
