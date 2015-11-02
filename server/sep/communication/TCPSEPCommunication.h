#pragma once

#include <thread>

#include "sep/communication/AbstractSEPCommunication.h"

class TCPSEPCommunication : public AbstractSEPCommunication
{
public:
    TCPSEPCommunication(uint32_t port);
    ~TCPSEPCommunication();

private:
    bool _is_running;
    std::thread _main_thread;

private:
    void run();
};
