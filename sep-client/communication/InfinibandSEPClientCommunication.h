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
