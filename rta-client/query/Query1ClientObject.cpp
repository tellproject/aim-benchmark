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
#include "query/Query1ClientObject.h"

#include <cassert>

Query1ClientObject::Query1ClientObject() :
    AbstractQueryClientObject(),
    m_aggrCount(0),
    m_aggrSum(0)
{}

void
Query1ClientObject::merge(const char *buffer, size_t bufferSize)
{
    assert(bufferSize == sizeof(uint64_t) + sizeof(uint32_t));
    m_aggrSum += *(reinterpret_cast<uint64_t *>(const_cast<char *>(buffer)));
    m_aggrCount += *(reinterpret_cast<uint32_t *>
                     (const_cast<char *>(&buffer[sizeof(uint64_t)])));
}

std::vector<char>
Query1ClientObject::getResult()
{
    std::vector<char> result(sizeof(double), '0');
    *(reinterpret_cast<double *>(result.data())) = ((double) m_aggrSum) /
            ((double) m_aggrCount);
    return std::move(result);
}
