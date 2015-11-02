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
#include "rta-client/RTAClient.h"

#include <cstdlib>
#include <cmath>
#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
#include <thread>

#include "communication/InfinibandRTAClientCommunication.h"
#include "communication/TCPRTAClientCommunication.h"
#include "util/dimension-tables-unique-values.h"

std::vector<std::unique_ptr<RTAClient> >
RTAClient::s_createRTAClients(std::vector<ClientWorkload> workloads,
                              std::string serverAddressesFile,
                              NetworkProtocol protocol)
{
    // create server addresses vector
    std::vector<ServerAddress> addresses;
    std::ifstream address_input_stream(serverAddressesFile);
    uint32_t count = 0;
    while (address_input_stream.good()) {
        std::string host("");
        uint32_t port(0);
        address_input_stream >> host;
        address_input_stream >> port;
        if (host.length() > 0) {
            addresses.emplace_back(host, port);
            count++;
        }
    }
    address_input_stream.close();

    // create clients
    std::vector<std::unique_ptr<RTAClient> > clients;
    clients.reserve(workloads.size());
    for (ClientWorkload& workload: workloads) {
        std::vector<std::string> workloadLines;
        std::ifstream workload_input_stream(workload.workloadFile);
        while (workload_input_stream.good()) {
            std::string workloadLine("");
            workload_input_stream >> workloadLine;
            if (workloadLine.length() > 0) {
                workloadLines.emplace_back(workloadLine);
            }
        }
        workload_input_stream.close();
        clients.emplace_back(new RTAClient(workload.numberOfThreads,
                                           std::move(workloadLines),
                                           addresses, protocol));
    }
    return clients;
}

RTAClient::RTAClient(uint8_t threads_num, std::vector<std::string> workload,
                     const std::vector<ServerAddress>& addresses,
                     NetworkProtocol protocol):

    m_numberOfThreads(threads_num),
    m_workload(std::move(workload)),
    m_workloadSize(m_workload.size()),
    m_isRunning(true),
    m_endedThreads(0)
{
    // initialize communication modules
    m_communicationModules.reserve(threads_num);
    switch (protocol) {
    case NetworkProtocol::TCP:
        for (uint8_t i=0; i<threads_num; ++i) {
            m_communicationModules.emplace_back(
                        new TCPRTAClientCommunication(addresses));
        }
        break;
    case NetworkProtocol::Infiniband:
        for (uint8_t i=0; i<threads_num; ++i) {
            m_communicationModules.emplace_back(
                        new InfinibandRTAClientCommunication(addresses));
        }
        break;
    }

    // start threads
    for (uint8_t i=0; i<threads_num; ++i) {
        new std::thread([&, i](){run(i);});
    }
}

void
RTAClient::stop()
{
    m_isRunning = false;
}

bool
RTAClient::hasEnded()
{
    return m_endedThreads == m_numberOfThreads;
}

std::vector<std::vector<double> >
RTAClient::getStatistics()
{
    std::vector<std::vector<double> > result(NUM_QUERY_TYPES);
    for (uint8_t i=0; i<result.size(); ++i) {
        result[i].push_back(m_queryCounters[i]);
        result[i].push_back(m_querySums[i] / ((double) m_queryCounters[i]));
        result[i].push_back(sqrt(
               m_querySumsSquared[i] / ((double) m_queryCounters[i])
               - result[i][1] * result[i][1]));
    }
    return std::move(result);
}

void
RTAClient::run(uint8_t thread_id)
{
    // initialize random elements
    std::default_random_engine rdom (rand() % 100);
    std::uniform_int_distribution<int> query_dist(0, m_workloadSize-1);
    std::uniform_int_distribution<int> q1_dist(2, 10);
    std::uniform_int_distribution<int> q2_dist(2, 10);
    std::uniform_int_distribution<int> q4_a_dist(2, 10);
    std::uniform_int_distribution<int> q4_b_dist(200000, 1500000);
    std::uniform_int_distribution<int> q5_a_dist(0, subscription_unique_type.size()-1);
    std::uniform_int_distribution<int> q5_b_dist(0, subscriber_unique_category_type.size()-1);
    std::uniform_int_distribution<int> q6_country_dist(0, region_unique_country.size()-1);
    std::uniform_int_distribution<int> q7_window_length_dist(0, 1);
    std::uniform_int_distribution<int> q7_subscr_value_type_dist(0, subscriber_unique_value_type.size()-1);

    // initialize local statistics
    uint32_t queryCounters[NUM_QUERY_TYPES] = {0};   // once for every query type
    double querySums[NUM_QUERY_TYPES] = {0};         // once for every query type
    double querySumsSquared[NUM_QUERY_TYPES] = {0};  // once for every query type

    // timing elements
    double responseTime;
    std::chrono::high_resolution_clock::time_point t1, t2;
    std::chrono::duration<double> timeSpan;

    // start at a random position in the workload
    uint8_t currentQueryIndex = query_dist(rdom);
    while (m_isRunning) {
        uint8_t currentQuery = std::stoi(m_workload[currentQueryIndex], nullptr);
        switch (currentQuery) {
        case 1:
            {
                uint32_t alpha = q1_dist(rdom);
                t1 = std::chrono::high_resolution_clock::now();
                auto result = m_communicationModules[thread_id]->processQuery1(alpha);
                t2 = std::chrono::high_resolution_clock::now();
                timeSpan = std::chrono::duration_cast
                        <std::chrono::duration<double>>(t2 - t1);
                LOG_INFO("Result of Q1: "
                         << *(reinterpret_cast<double *>(result.data())));
                break;
            }
        case 2:
            {
                uint32_t alpha = q2_dist(rdom);
                t1 = std::chrono::high_resolution_clock::now();
                auto result = m_communicationModules[thread_id]->processQuery2(alpha);
                t2 = std::chrono::high_resolution_clock::now();
                timeSpan = std::chrono::duration_cast
                        <std::chrono::duration<double>>(t2 - t1);
                LOG_INFO("Result of Q2: "
                         << *(reinterpret_cast<double *>(result.data())));
                break;
            }
        case 3:
            {
                t1 = std::chrono::high_resolution_clock::now();
                auto result = m_communicationModules[thread_id]->processQuery3();
                t2 = std::chrono::high_resolution_clock::now();
                timeSpan = std::chrono::duration_cast
                        <std::chrono::duration<double>>(t2 - t1);
                LOG_INFO("Result size of Q3: " << result.size());
                break;
            }
        case 4: //First join query
            {
                uint32_t alpha = q4_a_dist(rdom);
                uint32_t beta = q4_b_dist(rdom);
                t1 = std::chrono::high_resolution_clock::now();
                auto result = m_communicationModules[thread_id]->processQuery4(alpha, beta);
                t2 = std::chrono::high_resolution_clock::now();
                timeSpan = std::chrono::duration_cast
                        <std::chrono::duration<double>>(t2 - t1);
                LOG_INFO("Result size of Q4: " << result.size());
                break;
            }
        case 5: //Second join query
            {
                uint32_t alpha = q5_a_dist(rdom);
                uint32_t beta = q5_b_dist(rdom);
                t1 = std::chrono::high_resolution_clock::now();
                auto result = m_communicationModules[thread_id]->processQuery5(alpha, beta);
                t2 = std::chrono::high_resolution_clock::now();
                timeSpan = std::chrono::duration_cast
                        <std::chrono::duration<double>>(t2 - t1);
                LOG_INFO("Result size of Q5 " << result.size());
                break;
            }
        case 6: //Third join query
            {
                uint16_t country_id = q6_country_dist(rdom);
                t1 = std::chrono::high_resolution_clock::now();
                auto result = m_communicationModules[thread_id]->processQuery6(country_id);
                t2 = std::chrono::high_resolution_clock::now();
                timeSpan = std::chrono::duration_cast
                        <std::chrono::duration<double>>(t2 - t1);
                LOG_INFO("Result size of Q6 " << result.size());
                break;
            }
        case 7: //Fourth join query
            {
                uint16_t window_length = q7_window_length_dist(rdom);
                uint16_t subscr_value_type = q7_subscr_value_type_dist(rdom);
                t1 = std::chrono::high_resolution_clock::now();
                auto result = m_communicationModules[thread_id]->processQuery7(window_length , subscr_value_type);
                t2 = std::chrono::high_resolution_clock::now();
                timeSpan = std::chrono::duration_cast
                        <std::chrono::duration<double>>(t2 - t1);
                LOG_INFO("Result size of Q7 " << result.size());
                break;
            }
        }
        responseTime = timeSpan.count() * 1000; // convert to ms

        // update statistics
        queryCounters[currentQuery-1]++;
        querySums[currentQuery-1] += responseTime;
        querySumsSquared[currentQuery-1] += responseTime*responseTime;

        // next iteration
        currentQueryIndex++;
        currentQueryIndex %= m_workloadSize;
    }

    //update glocal statistics with local statistics
    {
        std::lock_guard<std::mutex> lock(m_statisticsUpdateMutex);
        for (uint i = 0; i < NUM_QUERY_TYPES; ++i) {
            m_queryCounters[i] += queryCounters[i];
            m_querySums[i] += querySums[i];
            m_querySumsSquared[i] += querySumsSquared[i];
        }
    }

    // experiment has finished
    ++m_endedThreads;
}
