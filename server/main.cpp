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
//#include <iostream>

//#include "rta/communication/InfinibandRTACommunication.h"
//#include "rta/communication/TCPRTACommunication.h"
//#include "rta/dimension_schema.h"
//#include "sep/communication/InfinibandSEPCommunication.h"
//#include "sep/communication/TCPSEPCommunication.h"
//#include "sep/schema_and_index_builder.h"
//#include "sep/sep.h"
//#include "sep_config.h"

//using namespace std;

///*
// * It prints all the configuration details for a run.
// */
//void printInfo(uint server_id, string sep_protocol, string rta_protocol,
//               const AIMSchema &schema, const CampaignIndex &campaign_index)
//{
//    cout << "Server id: " << server_id << endl;
//    cout << "Server number: " << SERVER_NUM << endl;
//    cout << "SEP Protocol: " << sep_protocol << endl;
//    cout << "RTA Protocol: " << rta_protocol << endl;
//    cout << "Records/bucket: " << RECORDS_PER_BUCKET << endl;
//    cout << "SEP Threads: " << SEP_THREAD_NUM << endl;
//    cout << "RTA Threads: " << RTA_THREAD_NUM << endl;
//    cout << "Total Subscribers: " << SUBSCR_NUM << endl;
//    cout << "Subscribers on this Server: " << SUBSCR_NUM / SERVER_NUM << endl;
//    cout << "Number of AM attrs: " << schema.numOfEntries() << endl;
//    cout << "Record size (bytes): " << schema.size() << endl;
//    cout << "Number of active campaigns: " << campaign_index.campaignNum() << endl;
//    cout << "Number of entry indexes: " << campaign_index.indexedNum() << endl;
//    cout << "Number of unindexed conjuncts: " << campaign_index.unindexedNum() << endl;
//}

//int main(int argc, const char* argv[])
//{
//    if (argc != 4 ) {
//        std::cout << "usage: ./aim <Server Id><SEP protocol><RTA protocol>"
//                  << std::endl;
//        return 1;
//    }
//    try {
//        SchemaAndIndexBuilder builder(SQLITE_DB);
//        AIMSchema aim_schema = builder.buildAIMSchema();
//        DimensionSchema dim_schema;

//#ifndef NDEBUG
//        cout << "DEBUG\n" << aim_schema << endl;
//#endif
//        CampaignIndex campaign_index = builder.buildCampaignIndex(aim_schema);
//        std::string sep_protocol(argv[2]);
//        std::string rta_protocol(argv[3]);
//        printInfo(stoi(argv[1]), sep_protocol, rta_protocol, aim_schema, campaign_index);

//        unique_ptr<AbstractSEPCommunication> sep_com;
//        unique_ptr<AbstractRTACommunication> rta_com;
//        if (sep_protocol.compare("Inf") == 0) {
//            sep_com.reset(new InfinibandSEPCommunication(SEP_PORT));
//        }
//        else {
//            sep_com.reset(new TCPSEPCommunication(SEP_PORT));
//        }
//        if (rta_protocol.compare("Inf") == 0) {
//            rta_com.reset(new InfinibandRTACommunication(RTA_PORT, RTA_THREAD_NUM, 1, aim_schema, dim_schema));
//        }
//        else {
//            rta_com.reset(new TCPRTACommunication(RTA_PORT, RTA_THREAD_NUM, 1, aim_schema, dim_schema));
//        }
//        Sep sep(stoi(argv[1]), aim_schema, dim_schema, campaign_index,
//                move(rta_com), move(sep_com));
//        sep.execute();
//    }
//    catch (std::exception& exeption) {
//        cerr << exeption.what() << endl;
//        return 3;
//    }
//    return 0;
//}
#include "Connection.hpp"
#include <crossbow/allocator.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/logger.hpp>
#include <telldb/TellDB.hpp>

#include <boost/asio.hpp>
#include <string>
#include <iostream>

using namespace crossbow::program_options;
using namespace boost::asio;

void accept(boost::asio::io_service &service,
        boost::asio::ip::tcp::acceptor &a,
        tell::db::ClientManager<void>& clientManager) {
    auto conn = new aim::Connection(service, clientManager);
    a.async_accept(conn->socket(), [conn, &service, &a, &clientManager](const boost::system::error_code &err) {
        if (err) {
            delete conn;
            LOG_ERROR(err.message());
            return;
        }
        conn->run();
        accept(service, a, clientManager);
    });
}

int main(int argc, const char** argv) {
    bool help = false;
    std::string host;
    std::string port("8713");
    std::string logLevel("DEBUG");
    crossbow::string commitManager;
    crossbow::string storageNodes;
    auto opts = create_options("aim_server",
            value<'h'>("help", &help, tag::description{"print help"}),
            value<'H'>("host", &host, tag::description{"Host to bind to"}),
            value<'p'>("port", &port, tag::description{"Port to bind to"}),
            value<'l'>("log-level", &logLevel, tag::description{"The log level"}),
            value<'c'>("commit-manager", &commitManager, tag::description{"Address to the commit manager"}),
            value<'s'>("storage-nodes", &storageNodes, tag::description{"Semicolon-separated list of storage node addresses"})
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
    if (numWarehouses == 0) {
        std::cerr << "Number of warehouses needs to be set" << std::endl;
        return 1;
    }

    crossbow::allocator::init();

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    tell::store::ClientConfig config;
    config.commitManager = config.parseCommitManager(commitManager);
    config.tellStore = config.parseTellStore(storageNodes);
    tell::db::ClientManager<void> clientManager(config);
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
        accept(service, a, clientManager);
        service.run();
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
