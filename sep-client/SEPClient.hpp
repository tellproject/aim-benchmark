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
#include <boost/asio.hpp>
#include <common/Protocol.hpp>
#include <random>
#include <chrono>
#include <deque>

#include <common/Util.hpp>

namespace aim {

using Clock = std::chrono::system_clock;

struct LogEntry {
    bool success;
    crossbow::string error;
    Command transaction;
    decltype(Clock::now()) start;
    decltype(start) end;
};

class SEPClient {
    using Socket = boost::asio::ip::tcp::socket;
    Socket mSocket;
    client::CommandsImpl mCmds;
    uint64_t mLowest;
    uint64_t mHighest;
    Random_t rnd;
    std::deque<LogEntry> mLog;
    decltype(Clock::now()) mEndTime;
public:
    SEPClient(boost::asio::io_service& service, uint64_t subscriberNum, uint64_t lowest, uint64_t highest, decltype(Clock::now()) endTime)
        : mSocket(service)
        , mCmds(mSocket)
        , mLowest(lowest)
        , mHighest(highest)
        , rnd(subscriberNum)
        , mEndTime(endTime)
    {}
    Socket& socket() {
        return mSocket;
    }
    const Socket& socket() const {
        return mSocket;
    }
    client::CommandsImpl& commands() {
        return mCmds;
    }
    void run();
    void populate();
    const std::deque<LogEntry>& log() const { return mLog; }
private:
    void populate(uint64_t lowest, uint64_t highest);
    template<Command C>
    void execute(const typename Signature<C>::arguments& arg);
};

}
