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
#include "InfinibandRTAClientCommunication.h"

#include "util/logger.h"

InfinibandRTAClientCommunication::InfinibandRTAClientCommunication(
        const std::vector<ServerAddress>& serverAddresses
    ) :
    AbstractRTAClientCommunication(serverAddresses),
    _next_result_symbol(GET_NEXT_RESULT_SYMBOL)
{
    _rdma_bufs.reserve(_numberOfServers);
    _infinibandInterfaces.reserve(_numberOfServers);
    for (int i = 0; i < _numberOfServers; ++i) {
        _infinibandInterfaces.emplace_back(new util::inf_client());
        _infinibandInterfaces[i]->connect_to(
                    _serverAddresses[i].host, _serverAddresses[i].port);
        _rdma_bufs.emplace_back(new util::buffer(RTA_REQUEST_BUFFER_SIZE));
    }
}

InfinibandRTAClientCommunication::~InfinibandRTAClientCommunication()
{}

void
InfinibandRTAClientCommunication::processQuery(uint32_t threadNumber,
                                            AbstractQueryClientObject &queryObject)
{
    auto* inf = _infinibandInterfaces[threadNumber].get();
    auto* buf = _rdma_bufs[threadNumber].get();
    util::inf_client::request crq(inf, buf);
    buf->set_data(_sendBuf, _sendBufSize);
    crq.send_request();
    crq.wait_for_response();

    uint32_t queryNumber = *reinterpret_cast<const uint32_t *>(crq.response());

    LOG_INFO ("Client got assigned query number" << queryNumber)

    // create next result message
    buf->set_data(reinterpret_cast<const char *>(&_next_result_symbol), sizeof(uint32_t));
    buf->set_data(sizeof(uint32_t), reinterpret_cast<const char *>(&queryNumber), sizeof(uint32_t));

    while (true) {
        util::inf_client::request crq(inf, buf);
        buf->set_data_size(2*sizeof(uint32_t));   //hack to reuse buffer content
        crq.send_request();
        crq.wait_for_response();
        if (crq.response_size() == 1 &&
                *(reinterpret_cast<const uint8_t *>(crq.response())) == GET_NEXT_RESULT_SYMBOL) {
            LOG_INFO ("Client got notified that query " << queryNumber
                      << " finshed")
            break;
        } else {
            // thread-safe operation
            std::lock_guard<std::mutex> lock(_queryObjectMutex);
            queryObject.merge(crq.response(), crq.response_size());
        }
        LOG_INFO ("Client has received a partial result for query "
                  << queryNumber)
    }
}
