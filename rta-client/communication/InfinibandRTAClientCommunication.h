#pragma once

#include "AbstractRTAClientCommunication.h"
#include "util/infiniband/inf_client.h"

class InfinibandRTAClientCommunication : public AbstractRTAClientCommunication
{
public:
    InfinibandRTAClientCommunication(
            const std::vector<ServerAddress>& serverAddresses);

    ~InfinibandRTAClientCommunication();

private:
    std::vector<std::unique_ptr<util::inf_client>> _infinibandInterfaces;
    std::mutex _queryObjectMutex;
    std::vector<std::unique_ptr<util::buffer>> _rdma_bufs;  //one buffer per thread
    const uint32_t _next_result_symbol;

    void processQuery(uint32_t threadNumber,
                AbstractQueryClientObject &queryObject) override;

};
