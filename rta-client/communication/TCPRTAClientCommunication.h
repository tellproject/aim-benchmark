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

#include <arpa/inet.h>
#include <array>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <netdb.h>
#include <mutex>
#include <sstream>
#include <sys/socket.h>

#include "AbstractRTAClientCommunication.h"

class TCPRTAClientCommunication : public AbstractRTAClientCommunication
{
public:
    TCPRTAClientCommunication(const std::vector<ServerAddress> & serverAddresses);

    ~TCPRTAClientCommunication();

private:
    std::vector<std::array<char, RTA_RESPONSE_BUFFER_SIZE>> m_recvBufers;   // one char [] per server
    std::vector<struct sockaddr_in> m_socketAddresses;                  // one sockaddr_in per server
    std::mutex m_queryObjectMutex;

    /**
     * @brief createAndConnectNewSocket
     * creates and connects a new socket for host and port
     * @param socketAddress
     * @return the socket descriptor and -1 if unsuccessful
     */
    int createAndConnectNewSocket(sockaddr_in socketAddress);

    void processQuery(uint32_t threadNumber,
                      AbstractQueryClientObject & queryObject) override;
};
