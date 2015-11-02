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

#include <atomic>
#include <memory>

#include "communication/AbstractRTAClientCommunication.h"
#include "util/logger.h"

//TODO: transmit strings

struct ClientWorkload {
    ClientWorkload(std::string workloadFile, uint8_t numberOfThreads):
           workloadFile(workloadFile), numberOfThreads(numberOfThreads) {}

    uint8_t numberOfThreads;
    std::string workloadFile;
};

class RTAClient
{
public:
    /**
     * @brief createClients
     * Takes a number of client workloads, a file with server addresses and
     * a communication type and creates a vector of Clients that execute
     * these workloads. Can be used as a convenience method by the main-class.
     * @param workloads
     * @param serverAddressesFile
     * @param communicationType
     * @return
     */
    static std::vector<std::unique_ptr<RTAClient>> s_createRTAClients(std::vector<ClientWorkload> workloads,
            std::string serverAddressesFile,
            NetworkProtocol protocol);

    RTAClient(uint8_t threads_num,
              std::vector<std::string> workload,
              const std::vector<ServerAddress>& addresses,
              NetworkProtocol protocol);

    ~RTAClient() = default;

    /**
     * @brief stop
     * sends the stop signal to all client threads, which means
     * they stop sending new queries and finish the query they are currently
     * working on.
     */
    void stop();

    /**
     * @brief hasEnded
     * returns true iff all threads have finished their last query to process.
     * @return
     */
    bool hasEnded();

    /**
     * @brief getStatistics
     * returns for each query i an array where the elements are defined as
     * follows:
     * [i][0]: count
     * [i][1]: avg RT
     * [i][2]: std (RT)
     * @return
     */
    std::vector<std::vector<double>> getStatistics();

private:
    // management
    uint8_t m_numberOfThreads;
    std::vector<std::unique_ptr<AbstractRTAClientCommunication> > m_communicationModules;
    std::vector<std::string> m_workload;
    uint32_t m_workloadSize;
    bool m_isRunning;
    std::atomic_uint_fast8_t m_endedThreads;

    // statistics
    uint32_t m_queryCounters[NUM_QUERY_TYPES] = {0};   // once for every query type
    double m_querySums[NUM_QUERY_TYPES] = {0};         // once for every query type
    double m_querySumsSquared[NUM_QUERY_TYPES] = {0};  // once for every query type
    std::mutex m_statisticsUpdateMutex;

    /**
     * @brief run
     * method that is executed in parallel by each thread as long is m_isRunning
     * @param thread identifier
     */
    void run(uint8_t thread_id);

};
