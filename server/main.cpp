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
#include "Connection.hpp"
#include <crossbow/allocator.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/logger.hpp>
#include <telldb/TellDB.hpp>

#include <boost/asio.hpp>
#include <string>
#include <iostream>
#include <thread>

#include "server/rta/dimension_schema.h"
#include "server/sep/schema_and_index_builder.h"

using namespace crossbow::program_options;
using namespace boost::asio;


void accept(boost::asio::io_service &service,
        boost::asio::ip::tcp::acceptor &a,
        tell::db::ClientManager<aim::Context>& clientManager,
        const AIMSchema &aimSchema,
        const DimensionSchema &dimensionSchema,
        unsigned eventBatchSize) {
    auto conn = new aim::Connection(service, clientManager, aimSchema, dimensionSchema, eventBatchSize);
    a.async_accept(conn->socket(), [conn, &service, &a, &clientManager, aimSchema, dimensionSchema, eventBatchSize](const boost::system::error_code &err) {
        if (err) {
            delete conn;
            LOG_ERROR(err.message());
            return;
        }
        conn->run();
        accept(service, a, clientManager, aimSchema, dimensionSchema, eventBatchSize);
    });
}

int main(int argc, const char** argv) {
    bool help = false;
    std::string host;
    std::string port("8713");
    std::string logLevel("DEBUG");
    std::string schemaFile("");
    crossbow::string commitManager;
    crossbow::string storageNodes;
    unsigned eventBatchSize = 100u;
    unsigned processingThreads = 1u;
    unsigned scanBlockSize = 4096u;
    auto opts = create_options("aim_server",
            value<'h'>("help", &help, tag::description{"print help"}),
            value<'H'>("host", &host, tag::description{"Host to bind to"}),
            value<'p'>("port", &port, tag::description{"Port to bind to"}),
            value<'l'>("log-level", &logLevel, tag::description{"The log level"}),
            value<'c'>("commit-manager", &commitManager, tag::description{"Address to the commit manager"}),
            value<'s'>("storage-nodes", &storageNodes, tag::description{"Semicolon-separated list of storage node addresses"}),
            value<'f'>("schema-file", &schemaFile, tag::description{"path to SqLite file that stores AIM schema"}),
            value<'b'>("batch-size", &eventBatchSize, tag::description{"size of event batches"}),
            value<'t'>("threads", &processingThreads, tag::description{"number of processing threads"}),
            value<'m'>("block-size", &processingThreads, tag::description{"size of scan memory blocks"})
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

    if (!schemaFile.size()) {
        std::cerr << "no schema file!\n";
        return 1;
    }

    SchemaAndIndexBuilder builder(schemaFile.c_str());
    AIMSchema aimSchema = builder.buildAIMSchema();
    DimensionSchema dimSchema;

    crossbow::allocator::init();

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    tell::store::ClientConfig config;
    config.commitManager = config.parseCommitManager(commitManager);
    config.tellStore = config.parseTellStore(storageNodes);
    tell::db::ClientManager<aim::Context> clientManager(config);
    clientManager.allocateScanMemory(
            config.tellStore.size() * processingThreads, scanBlockSize);
    try {
        io_service service;
        boost::asio::io_service::work work(service);
        ip::tcp::acceptor a(service);
        boost::asio::ip::tcp::acceptor::reuse_address option(true);
        ip::tcp::resolver resolver(service);
        ip::tcp::resolver::iterator iter;
        if (host == "") {
            iter = resolver.resolve(ip::tcp::resolver::query(port));
        } else {
            iter = resolver.resolve(ip::tcp::resolver::query(host, port));
        }
        ip::tcp::resolver::iterator end;
        for (; iter != end; ++iter) {
            boost::system::error_code err;
            auto endpoint = iter->endpoint();
            auto protocol = iter->endpoint().protocol();
            a.open(protocol);
            a.set_option(option);
            a.bind(endpoint, err);
            if (err) {
                a.close();
                LOG_WARN("Bind attempt failed " + err.message());
                continue;
            }
            break;
        }
        if (!a.is_open()) {
            LOG_ERROR("Could not bind");
            return 1;
        }
        a.listen();
        // we do not need to delete this object, it will delete itself
        accept(service, a, clientManager, aimSchema, dimSchema, eventBatchSize);

        std::vector<std::thread> threads;
        threads.reserve(processingThreads-1);
        for (unsigned i = 0; i < processingThreads-1; ++i)
            threads.emplace_back([&service]{service.run();});
        service.run();
        for (auto &thread: threads)
            thread.join();


    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
