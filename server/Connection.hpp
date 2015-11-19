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
#include <boost/asio/steady_timer.hpp>

#include <common/Protocol.hpp>

#include <telldb/TellDB.hpp>
#include <tellstore/ScanMemory.hpp>

#include "server/sep/aim_schema.h"
#include "server/rta/dimension_schema.h"
#include "Transactions.hpp"

namespace aim {

struct Context {
    bool isInitialized = false;

    tell::store::ScanMemoryManager *scanMemoryMananger;

    std::vector<std::pair<id_t, AIMSchemaEntry>> tellIDToAIMSchemaEntry;

    id_t subscriberId;
    id_t timeStampId;

    id_t callsSumLocalWeek;
    id_t callsSumAllWeek;
    id_t callsSumAllDay;

    id_t durSumAllWeek;
    id_t durSumAllDay;
    id_t durSumLocalWeek;

    id_t durMaxLocalWeek;
    id_t durMaxLocalDay;
    id_t durMaxDistantWeek;
    id_t durMaxDistantDay;

    id_t costMaxAllWeek;
    id_t costSumAllWeek;
    id_t costSumAllDay;
    id_t costSumLocalWeek;
    id_t costSumDistantWeek;

    id_t subscriptionTypeId;

    id_t regionZip;
    id_t regionCity;
    id_t regionCountry;
    id_t regionRegion;

    id_t valueTypeId;

};

class CommandImpl;

class Connection {
    boost::asio::ip::tcp::socket mSocket;
    std::unique_ptr<CommandImpl> mImpl;
public:
    Connection(boost::asio::io_service& service, tell::db::ClientManager<Context>& clientManager,
               const AIMSchema &aimSchema, size_t processingThreads,
               unsigned eventBatchSize);
    ~Connection();
    decltype(mSocket)& socket() { return mSocket; }
    void run();
};

class UdpServer {
    boost::asio::ip::udp::socket mSocket;
    boost::asio::steady_timer mTimer;
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
        , mTimer(service)
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
private:
    void listen();
    void startTimer();
};

} // namespace aim

