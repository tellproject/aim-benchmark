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
