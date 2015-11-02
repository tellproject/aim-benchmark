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
