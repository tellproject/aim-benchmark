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
#include <string>
#include <thread>
#include <boost/asio.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/logger.hpp>

#include <kudu/client/client.h>

#include <common/Protocol.hpp>
#include "kudu.hpp"
#include "CreateSchemaKudu.hpp"
#include "PopulateKudu.hpp"
#include "TransactionsKudu.hpp"

using namespace crossbow::program_options;
using namespace boost::asio;

namespace aim {

void assertOk(kudu::Status status) {
    if (!status.ok()) {
        LOG_ERROR("ERROR from Kudu: %1%", status.message().ToString());
        throw std::runtime_error(status.message().ToString().c_str());
    }
}

using Session = std::tr1::shared_ptr<kudu::client::KuduSession>;

class Connection {
    boost::asio::ip::tcp::socket mSocket;
    server::Server<Connection> mServer;
    Session mSession;
    Populator mPopulator;
    Transactions mTxs;
    int mPartitions;
public:
    Connection(boost::asio::io_service& service, kudu::client::KuduClient& client, int partitions)
        : mSocket(service)
        , mServer(*this, mSocket)
        , mSession(client.NewSession())
        , mTxs(numWarehouses)
        , mPartitions(partitions)
    {
        assertOk(mSession->SetFlushMode(kudu::client::KuduSession::MANUAL_FLUSH));
        mSession->SetTimeoutMillis(60000);
    }
    ~Connection() = default;
    decltype(mSocket)& socket() { return mSocket; }
    void run() {
        mServer.run();
    }

    void close() {
        delete this;
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::EXIT, void>::type
    execute(const Callback callback) {
        mServer.quit();
        callback();
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::CREATE_SCHEMA, void>::type
    execute(std::tuple<int16_t, bool> args, const Callback& callback) {
        std::cout << "CreateSchema(" << std::get<0>(args) << ", " << std::get<1>(args) << ")";
        std::cout.flush();
        createSchema(*mSession, std::get<0>(args), mPartitions, std::get<1>(args));
        callback(std::make_tuple(true, crossbow::string()));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::POPULATE_WAREHOUSE, void>::type
    execute(std::tuple<int16_t, bool> args, const Callback& callback) {
        mPopulator.populateWarehouse(*mSession, std::get<0>(args), std::get<1>(args));
        callback(std::make_tuple(true, crossbow::string()));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::POPULATE_DIM_TABLES, void>::type
    execute(bool args, const Callback& callback) {
        mPopulator.populateDimTables(*mSession, args);
        callback(std::make_tuple(true, crossbow::string()));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::NEW_ORDER, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTxs.newOrderTransaction(*mSession, args));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::PAYMENT, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTxs.payment(*mSession, args));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::ORDER_STATUS, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTxs.orderStatus(*mSession, args));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::DELIVERY, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTxs.delivery(*mSession, args));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::STOCK_LEVEL, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTxs.stockLevel(*mSession, args));
    }
};

void accept(io_service& service, ip::tcp::acceptor& a, kudu::client::KuduClient& client, int16_t numWarehouses, int partitions) {
    auto conn = new Connection(service, client, numWarehouses, partitions);
    a.async_accept(conn->socket(), [&, conn, numWarehouses, partitions](const boost::system::error_code& err) {
        if (err) {
            delete conn;
            LOG_ERROR(err.message());
            return;
        }
        conn->run();
        accept(service, a, client, numWarehouses, partitions);
    });
}

}

int main(int argc, const char** argv) {
    bool help = false;
    std::string host;
    std::string port("8713");
    std::string udpPort("8714");
    std::string logLevel("DEBUG");
    std::string schemaFile("");
    crossbow::string commitManager;
    crossbow::string storageNodes;
    unsigned eventBatchSize = 100u;
    unsigned networkThreads = 1u;
    unsigned processingThreads = 2u;
    unsigned scanBlockNumber = 1;
    unsigned scanBlockSize = 0x6400000;
    int partitions = -1;
    auto opts = create_options("aim_server",
            value<'h'>("help", &help, tag::description{"print help"}),
            value<'H'>("host", &host, tag::description{"Host to bind to"}),
            value<'p'>("port", &port, tag::description{"Port to bind to"}),
            value<'u'>("udp-port", &udpPort, tag::description{"Udp-port to receive events"}),
            value<'P'>("partitions", &partitions, tag::description{"Number of partitions per table"}),
            value<'l'>("log-level", &logLevel, tag::description{"The log level"}),
            value<'c'>("commit-manager", &commitManager, tag::description{"Address to the commit manager"}),
            value<'s'>("storage-nodes", &storageNodes, tag::description{"Semicolon-separated list of storage node addresses"}),
            value<'f'>("schema-file", &schemaFile, tag::description{"path to SqLite file that stores AIM schema"}),
            value<'b'>("batch-size", &eventBatchSize, tag::description{"size of event batches"}),
            value<'n'>("network-threads", &networkThreads, tag::description{"number of (TCP) networking threads"}),
            value<'t'>("processing-threads", &processingThreads, tag::description{"number of (Infiniband) processing threads"}),
            value<'M'>("block-number", &scanBlockNumber, tag::description{"number of scan memory blocks"}),
            value<'m'>("block-size", &scanBlockSize, tag::description{"size of scan memory blocks"})
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

    if (partitions == -1) {
        std::cerr << "Number of partitions needs to be set" << std::endl;
        return 1;
    }

    SchemaAndIndexBuilder builder(schemaFile.c_str());
    AIMSchema aimSchema = builder.buildAIMSchema();

    crossbow::allocator::init();

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    tell::store::ClientConfig config;
    config.numNetworkThreads = processingThreads;
    config.commitManager = config.parseCommitManager(commitManager);
    config.tellStore = config.parseTellStore(storageNodes);
    tell::db::ClientManager<aim::Context> clientManager(config);
    clientManager.allocateScanMemory(
            config.tellStore.size() * processingThreads * scanBlockNumber, scanBlockSize);
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
        // Connect to Kudu
        kudu::client::KuduClientBuilder clientBuilder;
        clientBuilder.add_master_server_addr(storageNodes.c_str());
        std::tr1::shared_ptr<kudu::client::KuduClient> client;
        aim::assertOk(clientBuilder.Build(&client));
        // we do not need to delete this object, it will delete itself
        aim::accept(service, a, *client, numWarehouses, partitions);
        std::vector<std::thread> threads;
        for (unsigned i = 0; i < numThreads; ++i) {
            threads.emplace_back([&service](){
                    service.run();
            });
        }
        for (auto& t : threads) {
            t.join();
        }

    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
