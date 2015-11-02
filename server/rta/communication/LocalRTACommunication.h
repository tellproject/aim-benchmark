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

#include "rta/communication/AbstractRTACommunication.h"

/**
 * @brief The LocalRTACommunication class implements the RTA communication
 * through local memory for the standalone aim server.
 */
class LocalRTACommunication : public AbstractRTACommunication
{
public:

    LocalRTACommunication(uint8_t scan_threads,
                               uint8_t communication_threads,
                               const AIMSchema& aim_schema,
                               const DimensionSchema& dim_schema);

    ~LocalRTACommunication();

public:

    /**
     * @brief createAndEnqueueQuery
     * @param msg
     * @param finished pointer to a boolean (represented as uint_8)
     * that will be set to true when query is finished
     */
    void createAndEnqueueQuery(char*& msg, uint8_t *finished);

protected:

    void workerThreadMethod();

private:

    /**
     * @brief workerThreadMethod
     * the method that worker threads execute
     */
    std::vector<std::thread> _workerThreads;

};
