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
#include <crossbow/allocator.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/logger.hpp>

#include <kudu/client/client.h>

#include <common/Protocol.hpp>
#include "kudu.hpp"
#include "CreateSchemaKudu.hpp"
#include "PopulateKudu.hpp"
#include "TransactionsKudu.hpp"

#include "server/rta/dimension_schema.h"
#include "server/sep/schema_and_index_builder.h"

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
    const AIMSchema &mAimSchema;
    int mPartitions;
public:
    Connection(boost::asio::io_service& service, kudu::client::KuduClient& client, const AIMSchema &aimSchema, int partitions)
        : mSocket(service)
        , mServer(*this, mSocket)
        , mSession(client.NewSession())
        , mTxs(aimSchema)
        , mAimSchema(aimSchema)
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
    typename std::enable_if<C == Command::PROCESS_EVENT, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        LOG_ERROR("PROCESS_EVENT must be called over udp");
        std::terminate();
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::CREATE_SCHEMA, void>::type
    execute(uint64_t args, const Callback callback) {
        std::cout << "CreateSchema";
        std::cout.flush();
        createSchema(*mSession, int64_t(args), mAimSchema, mPartitions);
        callback(std::make_tuple(true, crossbow::string()));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::POPULATE_TABLE, void>::type
    execute(std::tuple<uint64_t /*lowestSubscriberNum*/, uint64_t /* highestSubscriberNum */> args, const Callback& callback) {
        mPopulator.populateWideTable(*mSession, mAimSchema, std::get<0>(args), std::get<1>(args));
        callback(std::make_tuple(true, crossbow::string()));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q1, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTxs.q1Transaction(*mSession, args));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q2, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTxs.q2Transaction(*mSession, args));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q3, void>::type
    execute(const Callback& callback) {
       callback(mTxs.q3Transaction(*mSession));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q4, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTxs.q4Transaction(*mSession, args));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q5, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTxs.q5Transaction(*mSession, args));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q6, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTxs.q6Transaction(*mSession, args));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q7, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        callback(mTxs.q7Transaction(*mSession, args));
    }
};

void accept(io_service& service, ip::tcp::acceptor& a, kudu::client::KuduClient& client,
        const AIMSchema &aimSchema, int partitions) {
    auto conn = new Connection(service, client, aimSchema, partitions);
    a.async_accept(conn->socket(), [&, conn, aimSchema, partitions](const boost::system::error_code& err) {
        if (err) {
            delete conn;
            LOG_ERROR(err.message());
            return;
        }
        conn->run();
        accept(service, a, client, aimSchema, partitions);
    });
}

thread_local unsigned UDP_THREAD_ID = 0;

class UdpServer {
    boost::asio::ip::udp::socket mSocket;
    Session mSession;
    Transactions mTxs;
    size_t mBufferSize;
    std::unique_ptr<char[]> mBuffer;
    unsigned mEventBatchSize;
    std::vector<std::vector<Event>> mEventBatches;

public:
    UdpServer(boost::asio::io_service& service,
              kudu::client::KuduClient& client,
              size_t numThreads,
              unsigned eventBatchSize,
              const AIMSchema &aimSchema)
        : mSocket(service)
        , mSession(client.NewSession())
        , mTxs(aimSchema)
        , mBufferSize(1024)
        , mBuffer(new char[mBufferSize])
        , mEventBatchSize(eventBatchSize)
        , mEventBatches(numThreads, std::vector<Event>())
    {
        for (auto& v : mEventBatches) {
            v.reserve(mEventBatchSize);
        }
    }

    ~UdpServer() = default;

    void bind(const std::string& host, const std::string& port) {
        using namespace boost::asio;
        mSocket.open(ip::udp::v4());
        ip::udp::resolver res(mSocket.get_io_service());
        ip::udp::resolver::iterator iter;
        if (host == "") {
            iter = res.resolve(ip::udp::resolver::query(port));
        } else {
            iter = res.resolve(ip::udp::resolver::query(host, port));
        }
        decltype(iter) end;
        for (; iter != end; ++iter) {
            boost::system::error_code err;
            auto endpoint = iter->endpoint();
            mSocket.bind(endpoint, err);
            if (err) {
                LOG_WARN("Bind attempt failed " + err.message());
                continue;
            }
            break;
        }
        if (!mSocket.is_open()) {
            LOG_ERROR("Could not bind");
            std::terminate();
        }
    }

    void run() {
        using err_code = boost::system::error_code;
        mSocket.async_receive(boost::asio::buffer(mBuffer.get(), mBufferSize), [this](const err_code& ec, size_t bt){
            if (ec) {
                LOG_ERROR(ec.message());
                run();
                return;
            }
    #ifndef NDEBUG
            size_t reqSize = *reinterpret_cast<size_t*>(mBuffer.get());
            assert(reqSize == bt);
            auto cmd = *reinterpret_cast<Command*>(mBuffer.get() + sizeof(size_t));
            assert (cmd == Command::PROCESS_EVENT);
    #endif
            crossbow::deserializer des(reinterpret_cast<uint8_t*>(mBuffer.get() + sizeof(size_t) + sizeof(Command)));
            Event ev;
            des & ev;
            auto &eventBatch = mEventBatches[UDP_THREAD_ID];
            if (eventBatch.size() >= mEventBatchSize) {
                std::vector<Event> events;
                events.swap(eventBatch);
                mTxs.processEvent(*mSession, events);
                eventBatch.reserve(mEventBatchSize);
            }
            eventBatch.push_back(ev);
            run();
        });
    }
};

} // namespace aim

int main(int argc, const char** argv) {
    bool help = false;
    std::string host;
    std::string port("8713");
    std::string udpPort("8714");
    std::string logLevel("DEBUG");
    std::string schemaFile("");
    crossbow::string storageNodes;
    unsigned eventBatchSize = 100u;
    unsigned numThreads = 4u;
    int partitions = -1;
    auto opts = create_options("aim_server",
            value<'h'>("help", &help, tag::description{"print help"}),
            value<'H'>("host", &host, tag::description{"Host to bind to"}),
            value<'p'>("port", &port, tag::description{"Port to bind to"}),
            value<'u'>("udp-port", &udpPort, tag::description{"Udp-port to receive events"}),
            value<'P'>("partitions", &partitions, tag::description{"Number of partitions per table"}),
            value<'l'>("log-level", &logLevel, tag::description{"The log level"}),
            value<'s'>("storage-nodes", &storageNodes, tag::description{"Semicolon-separated list of storage node addresses"}),
            value<'f'>("schema-file", &schemaFile, tag::description{"path to SqLite file that stores AIM schema"}),
            value<'b'>("batch-size", &eventBatchSize, tag::description{"size of event batches"}),
            value<'n'>("network-threads", &numThreads, tag::description{"number of (TCP) networking threads"})
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
        aim::accept(service, a, *client, aimSchema, partitions);

        aim::UdpServer udpServer(service, *client, numThreads, eventBatchSize, aimSchema);
        udpServer.bind(host, udpPort);
        udpServer.run();

        std::vector<std::thread> threads;
        for (unsigned i = 0; i < numThreads; ++i) {
            threads.emplace_back([&service, i](){
                    UDP_THREAD_ID = i;
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
