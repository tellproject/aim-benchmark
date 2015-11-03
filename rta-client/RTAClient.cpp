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
#include "RTAClient.hpp"
#include <common/Protocol.hpp>
#include <crossbow/logger.hpp>

using err_code = boost::system::error_code;

namespace aim {

template<Command C>
void Client::execute(const typename Signature<C>::arguments& arg) {
    auto now = Clock::now();
    if (now > mEndTime) {
        // Time's up
        // benchmarking finished
        return;
    }
    mCmds.execute<C>([this, now](const err_code& ec, typename Signature<C>::result result){
        if (ec) {
            LOG_ERROR("Error: " + ec.message());
            return;
        }
        auto end = Clock::now();
        mLog.push_back(LogEntry{result.success, result.error, C, now, end});
        run();
    }, arg);
}

void Client::run() {
    uint8_t currentQuery = mWorkload[mCurrentQueryIdx];
    mCurrentQueryIdx = (mCurrentQueryIdx + 1) / mWorkload.size();
    switch (currentQuery) {
    case 1:
        {
            LOG_DEBUG("Start Query 1");
            Q1In args;
            rnd.randomQ1(args);
            execute<Command::Q1>(args);
            break;
        }
    case 2:
        {
            LOG_DEBUG("Start Query 2");
            Q2In args;
            rnd.randomQ2(args);
            execute<Command::Q2>(args);
            break;
        }
    case 3:
        {
            LOG_DEBUG("Start Query 3");
            execute<Command::Q3>(null);
            break;
        }
    case 4:
        {
            LOG_DEBUG("Start Query 4");
            Q4In args;
            rnd.randomQ4(args);
            execute<Command::Q4>(args);
            break;
        }
    case 5:
        {
            LOG_DEBUG("Start Query 5");
            Q5In args;
            rnd.randomQ5(args);
            execute<Command::Q5>(args);
            break;
        }
    case 6:
        {
            LOG_DEBUG("Start Query 6");
            Q6In args;
            rnd.randomQ6(args);
            execute<Command::Q6>(args);
            break;
        }
    case 7:
        {
            LOG_DEBUG("Start Query 7");
            Q7In args;
            rnd.randomQ7(args);
            execute<Command::Q7>(args);
            break;
        }
    }
}

} // aim

