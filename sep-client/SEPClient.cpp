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
#include "SEPClient.hpp"
#include <common/Protocol.hpp>
#include <crossbow/logger.hpp>

using err_code = boost::system::error_code;

namespace aim {

void PopulationClient::populate() {
    LOG_INFO("Populating from %1% to %2%", mLowest, mHighest);
    populate(mLowest, mHighest);
}

void PopulationClient::populate(uint64_t lowest, uint64_t highest) {
    auto start = lowest;
    auto end = start + 1000 < highest ? (start + 1000) : highest;
    mCmds.execute<Command::POPULATE_TABLE>(
            [this, start, end, highest](const err_code& ec, const std::tuple<bool, crossbow::string>& res){
                if (ec) {
                    LOG_ERROR(ec.message());
                    return;
                }
                if (!std::get<0>(res)) {
                    LOG_ERROR(std::get<1>(res));
                    return;
                }
                LOG_INFO(("Populated Table, subsriber " + crossbow::to_string(start)
                          + " to " + crossbow::to_string(end)));
                if (end < highest) {
                    populate(end + 1, highest);
                }
            },
            std::make_tuple(start, end));
}

SEPClient::SEPClient(SEPClient&&) = default;

void SEPClient::run(unsigned messageRate) {
    if (Clock::now() > mEndTime) return;
    Event e;
    e.caller_id = rnd.randomWithin<int32_t>(mLowest, mHighest);
    rnd.randomEvent(e);
    crossbow::sizer sz;
    sz & sz.size;
    sz & Command::PROCESS_EVENT;
    sz & e;
    auto buf = new uint8_t[sz.size];
    crossbow::serializer ser(buf);
    ser & sz.size;
    ser & Command::PROCESS_EVENT;
    ser & e;
    ser.buffer.release();
    mSocket.async_send(boost::asio::buffer(buf, sz.size), [this, buf](const boost::system::error_code& ec, size_t bt) {
        if (ec) {
            LOG_ERROR("ERROR while sending event: %1%: %2%", ec.value(), ec.message());
        }
        delete[] buf;
    });
    mTimer->expires_from_now(std::chrono::microseconds(1000000/messageRate));
    mTimer->async_wait([this, messageRate](const boost::system::error_code& ec) {
        if (ec) {
            LOG_ERROR("FATAL: ABORT IN TIMER");
            std::terminate();
        }
        run(messageRate);
    });
    //execute<Command::PROCESS_EVENT>(e);
    ++mNumEvents;
}

} // aim

