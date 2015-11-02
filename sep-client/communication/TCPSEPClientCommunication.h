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
