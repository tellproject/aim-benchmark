#include "sep-client/communication/TCPSEPClientCommunication.h"

#include <arpa/inet.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>

#include "util/logger.h"

TCPSEPClientCommunication::TCPSEPClientCommunication(
        const std::vector<ServerAddress>& server_addresses,
        ulong wait_time, uint thread_id):

    AbstractSEPClientCommunication(server_addresses, wait_time, thread_id)
{
    for (int i = 0; i < _number_of_servers; ++i) {
        _socket_addresses.emplace_back();
        sockaddr_in & server_address = _socket_addresses[i];
        server_address.sin_family = AF_INET;
        server_address.sin_port = _server_addresses[i].port;
        if(inet_pton(AF_INET, _server_addresses[i].host.c_str(),
                     &server_address.sin_addr) <= 0)
        {
            LOG_ERROR("inet_pton error occured")
            exit(-1);
        }
    }
    _main_thread = std::thread([&]{this->run();});
}

void
TCPSEPClientCommunication::waitFor()
{
     _main_thread.join();
}

int
TCPSEPClientCommunication::_create_and_connect_new_socket(
        sockaddr_in socket_address)
{
    int socket_descriptor = 0;
    LOG_INFO("CLient tries to connet on port" << socket_address.sin_port)

    // create socket
    if((socket_descriptor = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        LOG_ERROR("Client failed to create socket")
    }

    // connect socket
    if((connect(socket_descriptor, (struct sockaddr *)&socket_address,
                sizeof(socket_address))) < 0)
    {
        LOG_ERROR("Client failed to connect on port " << socket_address.sin_port)
    }

    LOG_INFO("Connection established, socket descriptor:" << socket_descriptor)

    //return socket
    return socket_descriptor;
}

void
TCPSEPClientCommunication::_send_event_to_server(uint32_t server_id)
{
    int socket_desc = _create_and_connect_new_socket(_socket_addresses[server_id]);
    write(socket_desc, _send_buf, sizeof(_send_buf));
    close(socket_desc);
}
