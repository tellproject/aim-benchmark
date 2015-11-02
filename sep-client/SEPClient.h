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
#include <vector>
#include <string>
#include "sep-client/communication/AbstractSEPClientCommunication.h"
#include "util/system-constants.h"

class SEPClient
{
public:
    /**
     * @brief SEPClient
     * creates a new SEPClient with the given number of threads, waiting time.
     * @param number_of_threads
     * @param wait_time
     * @param server_addresses_file
     * @param communication_type
     */
    SEPClient(uint8_t threads_num,
              ulong wait_time,
              std::string server_addresses_file,
              NetworkProtocol protocol);

    ~SEPClient();

    /**
     * @brief stop
     * sends the stop signal to all client threads, which means
     * they stop sending new queries and finish the query they are currently
     * working on.
     */
    void stop();

    /**
     * @brief waitFor
     * wait until all threads have ended.
     */
    void waitFor();

    /**
     * @brief getSentEvents
     * returns the total number of sent events by all threads.
     * @return
     */
    ulong getSentEvents();

private:
    bool _have_joined;
    uint8_t _threads_num;
    std::vector<std::unique_ptr<AbstractSEPClientCommunication> > _communication_modules;

};
