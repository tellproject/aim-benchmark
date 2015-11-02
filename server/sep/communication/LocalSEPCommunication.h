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

#include <mutex>

#include "sep/communication/AbstractSEPCommunication.h"

/**
 * @brief The LocalSEPCommunication class
 * is used for the standalone server version where events are communicated through local memory
 */
class LocalSEPCommunication : public AbstractSEPCommunication
{
public:
    LocalSEPCommunication():
        AbstractSEPCommunication(0),
        queue_mutex()
    {}

    ~LocalSEPCommunication(){}

public:
    void queueEvent(const Event& event)
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        size_t queue_id = event.callerId() % _queues.size();
        __queueEvent(queue_id, event);
    }

private:
    std::mutex queue_mutex;
};
