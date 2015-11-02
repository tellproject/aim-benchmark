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
