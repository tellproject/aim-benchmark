#pragma once

#include <inttypes.h>
#include "sep/utils.h"

/*
 * This class wraps an actual data type. It contains the size of the data type
 * and also it's name.
 */
class TypeDescriptor
{
public:
    TypeDescriptor(DataType type);

public:
    DataType type() const;
    uint8_t size() const;

private:
    DataType _type;     //INT, UINT, ULONG, DOUBLE
    uint8_t _size;
};

inline DataType
TypeDescriptor::type() const
{
    return _type;
}

inline uint8_t
TypeDescriptor::size() const
{
    return _size;
}
