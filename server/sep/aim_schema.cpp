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
#include "aim_schema.h"

/*
 * Timestamp of last event is written once at the beginning of the record.
 * Consequently the first attribute comes after the sizeof of the timestamp.
 */
AIMSchema::AIMSchema()
{
    _size = sizeof(Timestamp);
    _offsets.push_back((uint16_t)_size);
}

AIMSchema::AIMSchema(AIMSchema &&other)
{
    _size = other._size;
    _offsets = std::move(other._offsets);
    _entries = std::move(other._entries);
}

/*
 * Move assignment operator
 */
AIMSchema&
AIMSchema::operator=(AIMSchema &&other)
{
    if (this != &other) {
        _size = other._size;
        _offsets = std::move(other._offsets);
        _entries = std::move(other._entries);
    }
    return *this;
}

/*
 * Every time a new entry is added to the schema we update the size of both the
 * full and the compact record and we also push the attribute's compact size
 * into the offsets vector. This way we know the offset of each attribute into
 * the record.
 */
void
AIMSchema::addEntry(const AIMSchemaEntry &se)
{
    _size += se.size();
    _offsets.push_back((uint16_t)_size);
    _entries.push_back(se);

    uint64_t id = _getEntryHash(se.valMetric(), se.valAggrFun(),
                                se.filterType(), se.winLength());
    _entry_to_offset[id] = *(_offsets.end() - 2);
    _entry_to_names[id] = "a_" + crossbow::to_string(_entry_count++);
}

uint64_t
AIMSchema::getOffset(Metric metric, AggrFun aggr_fun, FilterType filter_type,
                     WindowLength window_size) const
{
    uint64_t id = _getEntryHash(metric, aggr_fun, filter_type, window_size);
    auto iter = _entry_to_offset.find(id);
    assert(iter != _entry_to_offset.end());
    return iter->second;
}

crossbow::string AIMSchema::getName(Metric metric, AggrFun aggr_fun, FilterType filter_type, WindowLength window_size) const
{
    uint64_t id = _getEntryHash(metric, aggr_fun, filter_type, window_size);
    auto iter = _entry_to_names.find(id);
    assert(iter != _entry_to_names.end());
    return iter->second;
}

uint64_t
AIMSchema::_getEntryHash(Metric metric, AggrFun aggr_fun,
                         FilterType filter_type,
                         WindowLength window_len) const
{
    return uint64_t(metric) +
           (uint64_t(aggr_fun) << 8) +
           (uint64_t(filter_type) << 16) +
           (uint64_t(window_len) << 24);
}

std::vector<uint16_t>
AIMSchema::sizes() const
{
    assert(_entries.size());
    std::vector<uint16_t> sizes;
    sizes.push_back(sizeof(Timestamp));
    for (auto& entry: _entries) {
        sizes.push_back(entry.size());
    }
    return std::move(sizes);
}

/*
 * Prints information related to the schema.
 */
std::ostream&
operator<<(std::ostream& out, const AIMSchema &schema)
{
    for (size_t i=0; i<schema.numOfEntries(); ++i) {
        const auto& se = schema[i];
        out << i + 1 << ": " ;
        out << stringAggrFun(se.valAggrFun()) << "("
            << ((se.filterType() == FilterType::NO) ? "" : stringFilter(se.filterType()) + " ")
            << stringMetric(se.valMetric()) << ") per "
            << stringWindowSize(se.winDuration()) << ", offset: "
            << schema.OffsetAt((uint)i) << "\n";
    }
    return out;
}
