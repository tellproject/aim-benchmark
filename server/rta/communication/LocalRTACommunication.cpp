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
#include "rta/communication/LocalRTACommunication.h"

LocalRTACommunication::LocalRTACommunication(uint8_t scan_threads,
                           uint8_t communication_threads,
                           const AIMSchema& aim_schema,
                           const DimensionSchema& dim_schema):
    AbstractRTACommunication(0,
                             scan_threads,
                             communication_threads,
                             aim_schema,
                             dim_schema)
{
    _workerThreads.reserve(communication_threads);
    for (uint8_t i = 0; i < communication_threads; i++) {
        _workerThreads.emplace_back([&, this](){workerThreadMethod();});
    }
}

LocalRTACommunication::~LocalRTACommunication(){
    _is_running = false;
    for (auto &thread: _workerThreads) {
        thread.join();
    }
}


void LocalRTACommunication::createAndEnqueueQuery(char *&msg, uint8_t *finished){
    assert(*finished == 0);
    std::vector<char> additional_args;
    additional_args.reserve(sizeof(uint8_t*));
    *(reinterpret_cast<uint8_t**>(additional_args.data())) = finished;
    {
        std::lock_guard<std::mutex> lock(_active_queries_mutex);
        _active_queries.emplace_back(
                    _createQueryObject(msg, std::move(additional_args))
                    .release());
    }
}

void LocalRTACommunication::workerThreadMethod(){
    while (_is_running) {
        auto query_object = _getNextQueryObject(); //get next query object to poll
        if (query_object != nullptr) {

            // poll the query object
            auto result = query_object->popResult();

            // if query object has delivered last result, notify client by
            // setting finished to true
            if (result.second == AbstractQueryServerObject::Status::DONE) {
                *(*reinterpret_cast<uint8_t**>(query_object->getAdditionalArgs())) = 1;
                delete query_object;
            }
        }
        else {
            std::this_thread::yield();
        }
    }
}
