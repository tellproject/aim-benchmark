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
#include "sep-client/communication/InfinibandSEPClientCommunication.h"

#include "util/logger.h"

InfinibandSEPClientCommunication::InfinibandSEPClientCommunication(
        const std::vector<ServerAddress>& server_addresses,
        ulong wait_time, uint thread_id):

    AbstractSEPClientCommunication(server_addresses, wait_time, thread_id)
{
    for (int i = 0; i < _number_of_servers; ++i) {
        _infiniband_interfaces.emplace_back(new util::inf_client());
        _infiniband_interfaces[i].get()->connect_to(
                    _server_addresses[i].host, _server_addresses[i].port);
    }
    _buf.reset(new util::buffer(sizeof(_send_buf)));
    _main_thread = std::thread([&]{this->run();});
}

void
InfinibandSEPClientCommunication::waitFor()
{
    _main_thread.join();
}

void
InfinibandSEPClientCommunication::_send_event_to_server(uint32_t server_id)
{
    auto* inf = _infiniband_interfaces[server_id].get();
    util::inf_client::request crq(inf, _buf.get());
    _buf.get()->set_data(_send_buf, sizeof(_send_buf));
    crq.send_request();
    crq.wait_for_response();
}
