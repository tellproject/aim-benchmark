#pragma once

#include <thread>

#include "sep/communication/AbstractSEPCommunication.h"
#include "util/infiniband/inf_server.h"

class InfinibandSEPCommunication : public AbstractSEPCommunication
{
public:
    InfinibandSEPCommunication(uint32_t port);
    ~InfinibandSEPCommunication();

private:
    bool _is_running;
    util::inf_server _infiniband;
    std::thread _main_thread;

private:
    void run();
};
