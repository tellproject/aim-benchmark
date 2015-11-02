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

#include "query/AbstractQueryServerObject.h"
#include "rta/communication/AbstractRTACommunication.h"

/**
 * @brief The TCPRTACommunication class implements the RTA communication
 * over TCP/IP connections. The communication threads can be blocked by
 * heavy messages that come from the same query object.
 * TODO: optimize this once we consider several communication threads
 */
class TCPRTACommunication : public AbstractRTACommunication
{
public:
    TCPRTACommunication(uint32_t port_num,
                        uint8_t scan_threads,
                        uint8_t communication_threads,
                        const AIMSchema& aim_schema,
                        const DimensionSchema& dim_schema);

    ~TCPRTACommunication();

protected:
    void run();

private:
    /**
     * @brief m_mainThread
     * handle for the main thread. The main thread is created and started
     * automatically by the constructor of the class.
     */
    std::thread _main_thread;

private:
    /**
     * @brief workerThreadMethod
     * the method that worker threads execute
     */
    void _workerThreadMethod();
};
