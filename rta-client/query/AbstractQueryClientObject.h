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

#include <cstdint>
#include <sys/types.h>
#include <vector>

#include "util/serialization.h"

/**
 * @brief The AbstractQueryClientObject class
 * This class is used to merge partial results coming from the server (as char*)
 * into a single, final result. It is important to be aware that we assume that
 * access to these objects are single-threaded, i.e. the object is not per se
 * thread-safe.
 */
class AbstractQueryClientObject
{
public:
    /**
     * @brief AbstractQueryClientObject
     * creates a new Abstract Query Client Object
     */
    AbstractQueryClientObject() = default;

    virtual ~AbstractQueryClientObject();

    /**
     * @brief merge
     * merges a partial query result into this final query object. Should be
     * called once for each partial result (from each server). This function
     * updates the internal state of the object. If necessary, bytes are copied
     * from tuple to another buffer.
     * @param buffer pointer to char * with the partial result
     * @param bufferSize size (in bytes) of the buffer
     */
    virtual void merge(const char *buffer, size_t bufferSize) = 0;

    /**
     * @brief getResult
     * Can be called at any time to get the aggregated result. However, in order
     * to get the correct final result, this function should only be called
     * after all partial results have been merged.
     * @param resultBuf reference to a pointer where the result should be stored.
     * @return the size of the result (in bytes)
     */
    virtual std::vector<char> getResult() = 0;
};
