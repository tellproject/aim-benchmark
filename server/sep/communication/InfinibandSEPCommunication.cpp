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
#include "util/logger.h"

#include "InfinibandSEPCommunication.h"

InfinibandSEPCommunication::InfinibandSEPCommunication(uint32_t port):
    AbstractSEPCommunication(port),
    _is_running(true),
    _infiniband(_port),
    _main_thread([&](){run();})
{
    LOG_INFO("Started listening for SEP requests on port " << port)
}

InfinibandSEPCommunication::~InfinibandSEPCommunication()
{
    _infiniband.stop();
    _is_running = false;
    _main_thread.join();
}

void
InfinibandSEPCommunication::run()
{

    char default_reply [1];
    default_reply[0] = '0';

    auto process_request = [&] (uint32_t qp_num, uint32_t msg_id, const char* buf,
            size_t size)
    {
        if (_is_running) {
            // immediately send void response to client
            Event event = Event::deserialize(buf);
            while(!_queues[event.callerId() % _queues.size()].write(std::move(event))) {

            }
            _infiniband.send_message(qp_num, msg_id, default_reply,
                    sizeof(default_reply));
        }
    };

    // this process run until m_isRunning is set to false
    _infiniband.run([](){}, process_request);
}
