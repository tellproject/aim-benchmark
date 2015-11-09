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
#include <crossbow/program_options.hpp>
#include <crossbow/logger.hpp>

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <string>
#include <iostream>
#include <cassert>
#include <fstream>
#include <thread>

#include "RTAClient.hpp"

using namespace crossbow::program_options;
using namespace boost::asio;
using err_code = boost::system::error_code;

std::vector<std::string> split(const std::string str, const char delim) {
    std::stringstream ss(str);
    std::string item;
    std::vector<std::string> result;
    while (std::getline(ss, item, delim)) {
        if (item.empty()) continue;
        result.push_back(std::move(item));
    }
    return result;
}

int main(int argc, const char** argv) {
    bool help = false;
    uint64_t numSubscribers = 10 * 1024 * 1024;
    std::string workloadList;
    std::string hostList;
    std::string port("8713");
    std::string logLevel("DEBUG");
    std::string outFile("out.csv");
    size_t numClients = 1;
    unsigned time = 5*60;
    unsigned processingThreads = 1u;
    auto opts = create_options("rta_client",
            value<'h'>("help", &help, tag::description{"print help"})
            , value<'H'>("hosts", &hostList, tag::description{"Comma-separated list of hosts"})
            , value<'l'>("log-level", &logLevel, tag::description{"The log level"})
            , value<'c'>("num-clients", &numClients, tag::description{"Number of Clients to run per host"})
            , value<'n'>("num-subscribers", &numSubscribers, tag::description{"Number of subscribers (data size)"})
            , value<'w'>("workload", &workloadList, tag::description{"Comma-separated list of query numbers (1 to 7)"})
            , value<'t'>("time", &time, tag::description{"Duration of the benchmark in seconds"})
            , value<'o'>("out", &outFile, tag::description{"Path to the output file"})
            , value<'m'>("block-size", &processingThreads, tag::description{"size of scan memory blocks"})
            );
    try {
        parse(opts, argc, argv);
    } catch (argument_not_found& e) {
        std::cerr << e.what() << std::endl << std::endl;
        print_help(std::cout, opts);
        return 1;
    }
    if (help) {
        print_help(std::cout, opts);
        return 0;
    }
    if (hostList.empty()) {
        std::cerr << "No host\n";
        return 1;
    }
    if (workloadList.empty()) {
        std::cerr << "No workload\n";
        return 1;
    }
    auto startTime = aim::Clock::now();
    auto endTime = startTime + std::chrono::seconds(time);
    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    try {
        auto hosts = split(hostList, ',');
        auto workloadStrings = split(workloadList, ',');
        std::vector<uint8_t> workload (workloadStrings.size());
        for (uint i = 0; i < workloadStrings.size(); ++i)
            workload[i] = std::stoi(workloadStrings[i]);

        io_service service;
        auto sumClients = hosts.size() * numClients;
        std::vector<aim::RTAClient> clients;
        clients.reserve(sumClients);
        for (decltype(sumClients) i = 0; i < sumClients; ++i) {
            clients.emplace_back(service, workload, numSubscribers, endTime);
        }

        for (size_t i = 0; i < hosts.size(); ++i) {
            auto h = hosts[i];
            auto addr = split(h, ':');
            assert(addr.size() <= 2);
            auto p = addr.size() == 2 ? addr[1] : port;
            ip::tcp::resolver resolver(service);
            ip::tcp::resolver::iterator iter;
            if (hostList == "") {
                iter = resolver.resolve(ip::tcp::resolver::query(port));
            } else {
                iter = resolver.resolve(ip::tcp::resolver::query(hostList, port));
            }
            for (unsigned j = 0; j < numClients; ++j) {
                LOG_INFO("Connected to client " + crossbow::to_string(i*numClients + j));
                boost::asio::connect(clients[i*numClients + j].socket(), iter);
            }
        }

        for (decltype(clients.size()) i = 0; i < clients.size(); ++i) {
            auto& client = clients[i];
            client.run();
        }

        std::vector<std::thread> threads;
        threads.reserve(processingThreads-1);
        for (unsigned i = 0; i < processingThreads-1; ++i)
            threads.emplace_back([&service]{service.run();});
        service.run();
        for (auto &thread: threads)
            thread.join();

        LOG_INFO("Done, writing results");
        std::ofstream out(outFile.c_str());
        out << "start,end,transaction,success,error\n";
        for (const auto& client : clients) {
            const auto& queue = client.log();
            for (const auto& e : queue) {
                crossbow::string tName;
                switch (e.transaction) {
                case aim::Command::Q1:
                    tName = "Q1";
                    break;
                case aim::Command::Q2:
                    tName = "Q3";
                    break;
                case aim::Command::Q3:
                    tName = "Q3";
                    break;
                case aim::Command::Q4:
                    tName = "Q4";
                    break;
                case aim::Command::Q5:
                    tName = "Q5";
                    break;
                case aim::Command::Q6:
                    tName = "Q6";
                    break;
                case aim::Command::Q7:
                    tName = "Q7";
                    break;
                default:
                    tName = "Unknown Transaction which should not happen in rta-client";
                    break;
                }
                out << std::chrono::duration_cast<std::chrono::seconds>(e.start - startTime).count()
                    << std::chrono::duration_cast<std::chrono::seconds>(e.end - startTime).count()
                    << tName
                    << (e.success ? "true" : "false")
                    << e.error
                    << std::endl;
            }
        }
        std::cout << '\a';
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}

