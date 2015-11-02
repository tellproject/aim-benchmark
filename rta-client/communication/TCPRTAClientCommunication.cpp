#include "TCPRTAClientCommunication.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

#include "util/logger.h"

TCPRTAClientCommunication::TCPRTAClientCommunication(
        const std::vector<ServerAddress> &serverAddresses):

    AbstractRTAClientCommunication(serverAddresses)
{
    for (int i = 0; i < _numberOfServers; ++i) {
        m_recvBufers.emplace_back();
        m_socketAddresses.emplace_back();
        sockaddr_in& serverAddress = m_socketAddresses[i];
        serverAddress.sin_family = AF_INET;
        serverAddress.sin_port = _serverAddresses[i].port;
        if(inet_pton(AF_INET, _serverAddresses[i].host.c_str(),
                     &serverAddress.sin_addr) <= 0)
        {
            LOG_ERROR("inet_pton error occured")
            exit(-1);
        }
    }
}

TCPRTAClientCommunication::~TCPRTAClientCommunication()
{}

int
TCPRTAClientCommunication::createAndConnectNewSocket(sockaddr_in socketAddress)
{
    int socketDescriptor = 0;
    LOG_INFO("CLient tries to connet on port" << socketAddress.sin_port);

    // create socket
    if((socketDescriptor = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        LOG_ERROR("Client failed to create socket");
    }

    // connect socket
    if((connect(socketDescriptor, (struct sockaddr *)&socketAddress,
                sizeof(socketAddress))) < 0)
    {
        LOG_ERROR("Client failed to connect on port " << socketAddress.sin_port);
    }

    LOG_INFO("Connection established, socket descriptor:" << socketDescriptor);

    //return socket
    return socketDescriptor;
}

void
TCPRTAClientCommunication::processQuery(uint32_t threadNumber,
                                     AbstractQueryClientObject& queryObject)
{
    // create socket
    int socketDescriptor = createAndConnectNewSocket(
                m_socketAddresses[threadNumber]);

    // send query and wait for result
    write(socketDescriptor, _sendBuf, _sendBufSize);
    char * partialResult = m_recvBufers[threadNumber].data();
    int numberOfBytesRead;

    while ((numberOfBytesRead = read(socketDescriptor,
            partialResult, RTA_RESPONSE_BUFFER_SIZE)) > 0) {

        LOG_INFO("TCP Client read " << numberOfBytesRead
                  << " bytes from socket " << socketDescriptor);

        // merge partial result
        {
            // thread-safe operation
            std::lock_guard<std::mutex> lock(m_queryObjectMutex);
            queryObject.merge(partialResult, (size_t) numberOfBytesRead);
        }
    }
    close(socketDescriptor);
}
