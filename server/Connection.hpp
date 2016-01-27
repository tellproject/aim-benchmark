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
#include <vector>
#include <boost/asio.hpp>

#include <common/Protocol.hpp>

#include <telldb/TellDB.hpp>

#include "server/sep/aim_schema.h"
#include "Transactions.hpp"

#include "AimContext.hpp"

namespace aim {

class CommandImpl;

class Connection {
    boost::asio::ip::tcp::socket mSocket;
    std::unique_ptr<CommandImpl> mImpl;
public:
    Connection(boost::asio::io_service& service, tell::db::ClientManager<Context>& clientManager,
               const AIMSchema &aimSchema);
    ~Connection();
    decltype(mSocket)& socket() { return mSocket; }
    void run();
};

class UdpServer {
    boost::asio::ip::udp::socket mSocket;
    tell::db::ClientManager<Context>& mClientManager;
    size_t mBufferSize;
    std::unique_ptr<char[]> mBuffer;
    Transactions mTransactions;
    unsigned mEventBatchSize;
    std::vector<std::vector<Event>> mEventBatches;
    std::vector<std::atomic<bool>*> mProcessingThreadFree;
public:
    UdpServer(boost::asio::io_service& service,
              tell::db::ClientManager<Context>& clientManager,
              size_t processingThreads,
              unsigned eventBatchSize,
              const AIMSchema &aimSchema)
        : mSocket(service)
        , mClientManager(clientManager)
        , mBufferSize(1024)
        , mBuffer(new char[mBufferSize])
        , mTransactions(aimSchema)
        , mEventBatchSize(eventBatchSize)
        , mEventBatches(processingThreads, std::vector<Event>())
        , mProcessingThreadFree(processingThreads, nullptr)
    {
        for (auto& a : mProcessingThreadFree) {
            a = new std::atomic<bool>(true);
        }
        for (auto& v : mEventBatches) {
            v.reserve(mEventBatchSize);
        }
    }
    ~UdpServer() {
        for (auto& a : mProcessingThreadFree) {
            delete a;
        }
    }
    void run();
    void bind(const std::string& addr, const std::string& port);
};

} // namespace aim

