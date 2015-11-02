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
