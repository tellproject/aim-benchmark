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

#include "SEPClient.hpp"

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

template<class Resolver, class Client>
void connectClients(std::vector<Client>& clients,
        const std::vector<std::string>& hosts,
        const std::string& port,
        boost::asio::io_service& service,
        size_t numClients,
        uint64_t numSubscribers,
        bool isUdp = false)
{
    using query = typename Resolver::query;
    auto sumClients = numClients * hosts.size();
    auto subscribersPerClient = numSubscribers / sumClients;
    for (decltype(sumClients) i = 0; i < sumClients; ++i) {
        clients.emplace_back(service, numSubscribers, subscribersPerClient * i + 1, subscribersPerClient * (i + 1), aim::Clock::now());
    }
    for (size_t i = 0; i < hosts.size(); ++i) {
        auto h = hosts[i];
        auto addr = split(h, ':');
        assert(addr.size() <= 3);
        auto p = addr.size() >= 2 ? addr[1] : port;
        if (isUdp) {
            p = addr.size() == 3 ? addr[2] : port;
        }
        Resolver resolver(service);
        typename Resolver::iterator iter;
        if (hosts.empty()) {
            iter = resolver.resolve(query(port));
        } else {
            iter = resolver.resolve(query(h, port));
        }
        for (unsigned j = 0; j < numClients; ++j) {
            LOG_INFO("Connected to client " + crossbow::to_string(i*numClients + j));
            boost::asio::connect(clients[i*numClients + j].socket(), iter);
        }
    }
}

void runPopulation(std::vector<aim::PopulationClient>& clients,
                   const std::vector<std::string>& hosts,
                   boost::asio::io_service& service,
                   const std::string& port,
                   size_t numClients,
                   uint64_t numSubscribers)
{
    using errcode = const boost::system::error_code&;
    clients.reserve(numClients * hosts.size());
    connectClients<boost::asio::ip::tcp::resolver>(clients, hosts, port, service, numClients, numSubscribers);
    clients[0].commands().execute<aim::Command::CREATE_SCHEMA>([&clients](errcode ec, const std::tuple<bool, crossbow::string>& res){
        if (ec) {
            LOG_ERROR("ERROR %1%: %2%", ec.value(), ec.message());
            return;
        }
        if (!std::get<0>(res)) {
            LOG_ERROR("ERROR: %1%", std::get<1>(res));
            return;
        }
        for (auto& c : clients) {
            c.populate();
        }
    });
}

int main(int argc, const char** argv) {
    bool help = false;
    bool populate = false;
    uint64_t numSubscribers = 10 * 1024 * 1024;
    std::string hostList;
    std::string port("8713");
    std::string udpPort("8714");
    std::string logLevel("DEBUG");
    std::string outFile("out.csv");
    size_t numClients = 1;
    unsigned time = 5*60;
    unsigned processingThreads = 1u;
    unsigned messageRate = 10000;
    auto opts = create_options("SEP_client",
            value<'h'>("help", &help, tag::description{"print help"})
            , value<'H'>("hosts", &hostList, tag::description{"Comma-separated list of hosts"})
            , value<'l'>("log-level", &logLevel, tag::description{"The log level"})
            , value<'c'>("num-clients", &numClients, tag::description{"Number of Clients to run per host"})
            , value<'P'>("populate", &populate, tag::description{"Populate the database"})
            , value<'n'>("num-subscribers", &numSubscribers, tag::description{"Number of subscribers (data size)"})
            , value<'t'>("time", &time, tag::description{"Duration of the benchmark in seconds"})
            , value<'o'>("out", &outFile, tag::description{"Path to the output file"})
            , value<'m'>("block-size", &processingThreads, tag::description{"size of scan memory blocks"})
            , value<'r'>("message-rate", &messageRate, tag::description{"Message rate in events/second"})
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


    auto startTime = aim::Clock::now();
    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    try {
        auto hosts = split(hostList, ',');
        io_service service;
        std::vector<aim::SEPClient> clients;
        std::vector<aim::PopulationClient> populationClients;
        if (populate) {
            runPopulation(populationClients, hosts, service, port, numClients, numSubscribers);
        } else {
            connectClients<boost::asio::ip::udp::resolver>(clients, hosts, udpPort, service, numClients, numSubscribers, true);
            for (auto& client : clients) {
                client.run(messageRate);
            }
        }

        std::vector<std::thread> threads;
        threads.reserve(processingThreads-1);
        for (unsigned i = 0; i < processingThreads-1; ++i)
            threads.emplace_back([&service]{service.run();});
        service.run();
        for (auto &thread: threads)
            thread.join();

        auto runTime = std::chrono::duration_cast<std::chrono::seconds>(aim::Clock::now() - startTime).count();
        size_t numEvents = 0;
        for (auto& c : clients) {
            numEvents += c.count();
        }

        LOG_INFO("Done, did send %1% events in %2% seconds", numEvents, runTime);
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}

