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
#include "TCPRTACommunication.h"

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

TCPRTACommunication::TCPRTACommunication(uint32_t port_num,
                                         uint8_t scan_threads,
                                         uint8_t communication_threads,
                                         const AIMSchema& aim_schema,
                                         const DimensionSchema& dim_schema):

    AbstractRTACommunication(port_num, scan_threads,
                             communication_threads, aim_schema, dim_schema),
    _main_thread([&](){run();})
{}

TCPRTACommunication::~TCPRTACommunication()
{
    _is_running = false;

    // connect to itself in order to unblock the main-thread
    sockaddr_in socket_address;
    socket_address.sin_family = AF_INET;
    socket_address.sin_port = _port_num;
    if(inet_pton(AF_INET, "127.0.0.1",
                 &socket_address.sin_addr) <= 0)
    {
        LOG_ERROR("inet_pton error occured")
        exit(-1);
    }

    int socket_descriptor = 0;

    // create socket
    if((socket_descriptor = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        LOG_ERROR("Server to itself failed to create socket");
    }

    // connect socket
    if((connect(socket_descriptor, (struct sockaddr *)&socket_address,
                sizeof(socket_address))) < 0)
    {
        LOG_ERROR("Server to itself failed to connect on port "
                  << socket_address.sin_port);
    }

    _main_thread.join();
    if (socket_descriptor >= 0)
        close(socket_descriptor);
}

void
TCPRTACommunication::_workerThreadMethod()
{
    while (_is_running) {
        auto query_object = _getNextQueryObject(); //get next query object to poll
        if (query_object != nullptr) {

            // poll the query object
            auto result = query_object->popResult();
            bool is_last = result.second == AbstractQueryServerObject::Status::DONE;
            int socket_descr =
                    *reinterpret_cast<int*>(query_object->getAdditionalArgs());

            // send result back to client
            write(socket_descr, result.first.data(), result.first.size());
            LOG_INFO("TCP Server wrote " << result.first.size() << " bytes to socket "
                         << socket_descr);

            // if query object has delivered last result, close & delete
            if (is_last) {
                delete query_object;
                close(socket_descr);
                socket_descr = -1;
            }
        }
        else {
            std::this_thread::yield();
        }
    }
}

void
TCPRTACommunication::run()
{
    // start communication helper threads
    std::vector<std::thread> helper_threads;
    helper_threads.reserve(_communication_threads);
    for (uint8_t i = 0; i < _communication_threads; ++i) {
        helper_threads.emplace_back([&](){_workerThreadMethod();});
    }

    // prepare for listening
    int listener_socket_descriptor = 0, connection_socket_descriptor = 0;
    char recv_buff[RTA_REQUEST_BUFFER_SIZE];

    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    server_address.sin_port = _port_num;

    listener_socket_descriptor = socket(AF_INET, SOCK_STREAM, 0);
    if (listener_socket_descriptor == -1) {
        LOG_ERROR("Bad listening socket descriptor"
                  << listener_socket_descriptor);
        exit(-1);
    }

    int i{1};
    if (setsockopt(listener_socket_descriptor, SOL_SOCKET, SO_REUSEADDR,
               &i, sizeof(int)) != 0) {
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
    while (_is_running) {
        connection_socket_descriptor = accept(listener_socket_descriptor,
            (struct sockaddr*)NULL, NULL);
        if (connection_socket_descriptor == -1) {
            LOG_ERROR("Accept error");
            exit(-1);
        }

        if (_is_running) {
            memset(recv_buff, '0', sizeof(recv_buff));
            read(connection_socket_descriptor, recv_buff, sizeof(recv_buff)-1);
            std::vector<char> additional_arguments(sizeof(int));
            *reinterpret_cast<int *>(additional_arguments.data()) =
                    connection_socket_descriptor;

            {
                std::lock_guard<std::mutex> lock(_active_queries_mutex);
                // Release the unique_ptr, it gets deleted in the workerThread
                _active_queries.emplace_back(
                            _createQueryObject(
                                recv_buff, std::move(additional_arguments)
                            ).release());
            }
        }
        else {
            // this is the last call from the server itself in order to unblock
            // the accept call
            close(connection_socket_descriptor);
        }
    }

    // clean-up helper thread objects
    for (auto & helper_thread : helper_threads) {
        helper_thread.join();
    }
    close(listener_socket_descriptor);
}
