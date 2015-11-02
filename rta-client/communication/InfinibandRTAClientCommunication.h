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
