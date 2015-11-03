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
//#include <chrono>
//#include <cstdint>
//#include <cstdio>
//#include <fstream>
//#include <iostream>
//#include <thread>
//#include <vector>

//#include "common/logger.h"
//#include "rta-client/RTAClient.hpp"
//#include "common/system-constants.h"

//void
//print_rta_client_args(int argc, const char* argv[])
//{
//    std::cout << "Experiment-duration: " << argv[1] << '\n'
//              << "Protocol: " << argv[2] << '\n'
//              << "Server-file: " << argv[3] << std::endl;
//    for (uint32_t i = 4; i < argc; i+=2) {
//        std::cout << "Workload-file: " << argv[i]
//                     << " Threads: " << argv[i+1] << std::endl;
//    }
//}

///*
// * Wait until experiment is finished.
// */
//void
//wait(uint64_t exp_duration)
//{
//    std::chrono::high_resolution_clock::time_point current, notified;
//    auto start = std::chrono::high_resolution_clock::now();
//    while (true) {
//        current = std::chrono::high_resolution_clock::now();
//        if (std::chrono::duration_cast<std::chrono::duration<double>>
//                (current - start).count() >= exp_duration) {
//            return;
//        }
//        else {
//            // notify user how much time of the experiment has passed
//            long diff = std::chrono::duration_cast<std::chrono::duration
//                    <double>>(current - notified).count();
//            if (diff >= (exp_duration/100.0)) {
//                notified = std::chrono::high_resolution_clock::now();
//                diff = std::chrono::duration_cast<std::chrono::duration
//                        <double>>(notified - start).count();
//                std::cout << (int) (100.0 * diff / exp_duration)
//                          << "% completed." << std::endl;
//            }
//            // sleep for a while
//            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
//        }
//    }
//}

///*
// * Stop the clients.
// */
//void
//stopRTAClients(std::vector<std::unique_ptr<RTAClient>> &clients)
//{
//    for (auto &client : clients) {
//        client->stop();
//    }
//    return;
//}

///*
// * Wait for clients to stop and output result.
// */
//void result(std::vector<std::unique_ptr<RTAClient>> &clients)
//{
//    int count = 0;
//    for (auto &client : clients) {
//        while (!client->hasEnded());
//        auto statistics = client->getStatistics();
//        std::cout << "Summary for workload " << (count+1) << " [ms]:" << std::endl;
//        for (uint8_t i=0; i<statistics.size(); ++i) {
//            std::cout << "Q" << (i+1) << ": " << statistics[i][1]
//                      << " (+/- " << statistics[i][2] << "), N = "
//                      << statistics[i][0] << std::endl;
//        }
//        ++count;
//    }
//}

//void printArgMsg()
//{
//    std::cout << "usage: rta-client <experiment-duration> <protocol>"
//              << "<server-file> [<workload-file> <number-of-threads>]+"
//              << "*experiment duration in seconds"
//              << "*protocol, [Inf, TCP]"
//              << "*server-file: each line contains 'host port'"
//              << "*each <workload-file> is executed with "
//              << "<number-of-threads> in parallel" << std::endl;
//}

//int main(int argc, const char* argv[])
//{
//    if (argc < 6 || (argc % 2 == 1)) {
//        printArgMsg();
//        return -1;
//    }

//#ifndef NDEBUG
//    std::cout << "DEBUG\n" << std::endl;
//#endif

//    print_rta_client_args(argc, argv);
//    uint64_t exp_duration(std::stol(argv[1]));
//    std::string protocol_string(argv[2]);
//    NetworkProtocol protocol = (protocol_string.compare("Inf")==0)?
//                NetworkProtocol::Infiniband:
//                NetworkProtocol::TCP;

//    std::string server_file(argv[3]);

//    // prepare client workloads
//    std::vector<ClientWorkload> client_workloads;
//    client_workloads.reserve((argc-4)/2);
//    for (uint32_t i = 4; i < argc; i+=2) {
//        client_workloads.emplace_back(argv[i], std::stoi(argv[i+1]));
//    }

//    // start clients
//    auto clients = RTAClient::s_createRTAClients(client_workloads, server_file, protocol);

//    std::cout << "Experiment started. Will wait for " << exp_duration
//              << " seconds..." << std::endl;

//    // wait until experiment is finished.
//    wait(exp_duration);

//    // stop the clients
//    stopRTAClients(clients);

//    // wait for clients to stop and output result
//    result(clients);
//}

#include <crossbow/program_options.hpp>
#include <crossbow/logger.hpp>

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <string>
#include <iostream>
#include <cassert>
#include <fstream>

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
    bool populate = false;
    uint64_t numSubscribers = 10 * 1024 * 1024;
    std::string workloadList;
    std::string hostList;
    std::string port("8713");
    std::string logLevel("DEBUG");
    std::string outFile("out.csv");
    size_t numClients = 1;
    unsigned time = 5*60;
    auto opts = create_options("tpcc_server",
            value<'h'>("help", &help, tag::description{"print help"})
            , value<'H'>("hosts", &hostList, tag::description{"Comma-separated list of hosts"})
            , value<'l'>("log-level", &logLevel, tag::description{"The log level"})
            , value<'c'>("num-clients", &numClients, tag::description{"Number of Clients to run per host"})
            , value<'P'>("populate", &populate, tag::description{"Populate the database"})
            , value<'n'>("num-subscribers", &numSubscribers, tag::description{"Number of subscribers (data size)"})
            , value<'w'>("workload", &workloadList, tag::description{"Comma-separated list of query numbers (1 to 7)"})
            , value<'t'>("time", &time, tag::description{"Duration of the benchmark in seconds"})
            , value<'o'>("out", &outFile, tag::description{"Path to the output file"})
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
        return 2;
    }
    auto startTime = tpcc::Clock::now();
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
        std::vector<aim::Client> clients;
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
        if (populate) {
            auto& cmds = clients[0].commands();
            cmds.execute<tpcc::Command::CREATE_SCHEMA>(
                    [&clients, numClients, wareHousesPerClient, numWarehouses](const err_code& ec,
                        const std::tuple<bool, crossbow::string>& res){
                if (ec) {
                    LOG_ERROR(ec.message());
                    return;
                }
                if (!std::get<0>(res)) {
                    LOG_ERROR(std::get<1>(res));
                    return;
                }
                for (auto& client : clients) {
                    client.populate();
                }
            });
        } else {
            for (decltype(clients.size()) i = 0; i < clients.size(); ++i) {
                auto& client = clients[i];
                client.run();
            }
        }
        service.run();

        //TODO: continue here

        LOG_INFO("Done, writing results");
        std::ofstream out(outFile.c_str());
        out << "start,end,transaction,success,error\n";
        for (const auto& client : clients) {
            const auto& queue = client.log();
            for (const auto& e : queue) {
                crossbow::string tName;
                switch (e.transaction) {
                case tpcc::Command::POPULATE_WAREHOUSE:
                    tName = "Populate";
                    break;
                case tpcc::Command::CREATE_SCHEMA:
                    tName = "Schema Create";
                    break;
                case tpcc::Command::STOCK_LEVEL:
                    tName = "Stock Level";
                    break;
                case tpcc::Command::DELIVERY:
                    tName = "Delivery";
                    break;
                case tpcc::Command::NEW_ORDER:
                    tName = "New Order";
                    break;
                case tpcc::Command::ORDER_STATUS:
                    tName = "Order Status";
                    break;
                case tpcc::Command::PAYMENT:
                    tName = "Payment";
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
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}

