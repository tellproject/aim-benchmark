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

#include <thread>

#include "sep-client/communication/AbstractSEPClientCommunication.h"

class TCPSEPClientCommunication : public AbstractSEPClientCommunication
{
public:
    TCPSEPClientCommunication(const std::vector<ServerAddress>&,
                              ulong wait_time,
                              uint thread_id);

    ~TCPSEPClientCommunication() = default;

public:
    void waitFor() override;

private:
    std::vector<struct sockaddr_in> _socket_addresses;  // one sockaddr_in per server
    std::thread _main_thread;

private:
    int _create_and_connect_new_socket(sockaddr_in socket_address);
    void _send_event_to_server(uint32_t server_id) override;

};
