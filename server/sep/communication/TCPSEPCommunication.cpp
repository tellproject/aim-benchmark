#include "TCPSEPCommunication.h"

#include <arpa/inet.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "util/logger.h"

TCPSEPCommunication::TCPSEPCommunication(uint32_t port):
    AbstractSEPCommunication(port),
    _is_running(true),
    _main_thread([&](){run();})
{}

TCPSEPCommunication::~TCPSEPCommunication()
{
    _is_running = false;

    // connect to itself in order to unblock the main-thread
    sockaddr_in socket_address;
    socket_address.sin_family = AF_INET;
    socket_address.sin_port = _port;
    if(inet_pton(AF_INET, "127.0.0.1",
                 &socket_address.sin_addr) <= 0)
    {
        LOG_ERROR("inet_pton error occured");
        exit(-1);
    }

    int socket_descriptor = 0;
    // create socket
    if((socket_descriptor = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        LOG_ERROR("Server to itself failed to create socket");
    }
    // connect socket
    if((connect(socket_descriptor, (struct sockaddr *) & socket_address,
                sizeof(socket_address))) < 0)
    {
        LOG_ERROR("Server to itself failed to connect on port "
                  << socket_address.sin_port);
    }
    _main_thread.join();
}

void
TCPSEPCommunication::run()
{
    // prepare for listening
    int listener_socket_descriptor = 0, connection_socket_descriptor = 0;
    char recv_buff[Event::s_serialized_event_size];

    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    server_address.sin_port = _port;

    listener_socket_descriptor = socket(AF_INET, SOCK_STREAM, 0);
    if (listener_socket_descriptor == -1) {
        LOG_ERROR("Bad listening socket descriptor"
                  << listener_socket_descriptor);
        exit(-1);
    }

    if (setsockopt(listener_socket_descriptor, SOL_SOCKET, SO_REUSEADDR,
               new int(1), sizeof(int)) != 0) {
        LOG_ERROR("Reuse Socket error");
        exit(-1);
    }

    if (bind(listener_socket_descriptor,
             reinterpret_cast<struct sockaddr*>(&server_address),
             sizeof(server_address)) != 0) {
        LOG_ERROR("Bind error: " << strerror(errno));
        exit(-1);
    }

    if(listen(listener_socket_descriptor, 10) != 0) { //up to 10 connections wait
        LOG_ERROR("Listen error. ");
    }

    LOG_INFO("Server started listening on " << server_address.sin_port);

    // listen to connecting clients
    while(_is_running) {
        connection_socket_descriptor = accept(listener_socket_descriptor,
            (struct sockaddr*)NULL, NULL);
        if (connection_socket_descriptor == -1) {
            LOG_ERROR("Accept error");
            exit(-1);
        }
        if (_is_running) {
            read(connection_socket_descriptor, recv_buff,
                sizeof(recv_buff)-1);
            Event event = Event::deserialize(recv_buff);
            while(!_queues[event.callerId() % _queues.size()].write(std::move(event))) {

            }
        }
        close(connection_socket_descriptor);
    }
}
