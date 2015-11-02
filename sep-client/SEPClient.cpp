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
#include "sep-client/SEPClient.h"

#include <chrono>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <random>
#include <thread>

#include "sep-client/communication/TCPSEPClientCommunication.h"
#include "sep-client/communication/InfinibandSEPClientCommunication.h"

SEPClient::SEPClient(uint8_t threads_num,
                     ulong wait_time,
                     std::string server_addresses_file,
                     NetworkProtocol protocol):
    _have_joined(false),
    _threads_num(threads_num)
{
    // create server addresses vector
    std::vector<ServerAddress> server_addresses;
    std::ifstream input_stream(server_addresses_file);
    uint32_t count = 0;
    while (input_stream.good()) {
        std::string host("");
        uint32_t port(0);
        input_stream >> host;
        input_stream >> port;
        if (host.length() > 0) {
            server_addresses.emplace_back(host, port);
            count++;
        }
    }
    input_stream.close();

    // initialize communication modules
    _communication_modules.reserve(threads_num);
    switch (protocol) {
    case NetworkProtocol::TCP:
        for (uint8_t thread_id=0; thread_id<threads_num; ++thread_id) {
            _communication_modules.emplace_back(
                new TCPSEPClientCommunication(
                            server_addresses, wait_time, thread_id));
        }
        break;
    case NetworkProtocol::Infiniband:
        for (uint8_t thread_id=0; thread_id<threads_num; ++thread_id) {
            _communication_modules.emplace_back(
                new InfinibandSEPClientCommunication(
                            server_addresses, wait_time, thread_id));
        }
        break;
    }
}

SEPClient::~SEPClient()
{
    this->stop();
    this->waitFor();
}

void
SEPClient::stop()
{
    for (auto &module: _communication_modules) {
        module->stopSending();
    }
}

void
SEPClient::waitFor()
{
    if (_have_joined) {
        return;
    }
    for (auto &module: _communication_modules) {
        module->waitFor();
    }
    _have_joined = true;
}

ulong
SEPClient::getSentEvents()
{
    ulong result = 0;
    for (auto &module: _communication_modules) {
        result += module->getSentEvents();
    }
    return result;
}
