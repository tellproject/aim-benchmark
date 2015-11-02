#pragma once

#include "sep-client/communication/AbstractSEPClientCommunication.h"
#include "util/infiniband/inf_client.h"

class InfinibandSEPClientCommunication : public AbstractSEPClientCommunication
{
public:
    InfinibandSEPClientCommunication(const std::vector<ServerAddress>&,
                                     ulong wait_time,
                                     uint thread_id);

    ~InfinibandSEPClientCommunication() = default;

public:
    void waitFor() override;

private:
    std::vector<std::unique_ptr<util::inf_client>> _infiniband_interfaces;
    std::unique_ptr<util::buffer> _buf;
    std::thread _main_thread;

private:
    void _send_event_to_server(uint32_t server_id) override;
};
