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
#include "InfinibandRTACommunication.h"

#include <cassert>

#include "util/logger.h"

InfinibandRTACommunication::InfinibandRTACommunication(uint32_t port_num,
                                                       uint8_t scan_threads,
                                                       uint8_t communication_threads,
                                                       const AIMSchema& aim_schema,
                                                       const DimensionSchema& dim_schema):
    AbstractRTACommunication(port_num, scan_threads,
                             communication_threads, aim_schema, dim_schema),
    _infiniband(port_num),	// try without port_num if it fails
    _main_thread(std::thread([&](){run();})),
    _query_references(),
    _finished_queries()
{
    LOG_INFO("Started listening for RTA requests on port " << port_num)
}

InfinibandRTACommunication::~InfinibandRTACommunication()
{
    _infiniband.stop();
    _is_running = false;
    _main_thread.join();
}

void
InfinibandRTACommunication::_sendResults()
{
        auto query_object = _getNextQueryObject();   //get next query object to poll
        if (query_object != nullptr) {
            char* additional_args;            // get qpNum and MessgeId
            //wait until new qpNum and messageId is set
            if ((additional_args = query_object->getAdditionalArgs()) == nullptr) {
                notifyForResults(query_object);
                return;
            }

            uint32_t q_num = *reinterpret_cast<uint32_t*>(additional_args);
            uint32_t msg_id = *reinterpret_cast<uint32_t*>(&additional_args[sizeof(uint32_t)]);

            query_object->setAdditionalArgs(vector<char>());
            uint32_t query_number = query_object->getQueryNr();
            bool has_finished = false;   // even when the query is done there is a last message to
                                        // be sent to indicate the end.
            {
                std::lock_guard<std::mutex> lock(_finished_queries_mutex);
                if (_finished_queries.find(query_number)
                        != _finished_queries.end()) {
                    has_finished = true;
                }
            }
            if (!has_finished) {
                // poll the query object
                auto result = query_object->popResult();
                LOG_INFO("Server sends back " << result.first.size()
                         << " bytes of result for query " << query_number);

                _infiniband.send_message(q_num, msg_id, result.first.data(),
                        result.first.size());

                // if query object has delivered last result,
                // mark as finished and notify for a last time
                if (result.second == AbstractQueryServerObject::Status::DONE) {
                    {
                        std::lock_guard<std::mutex> lock(_finished_queries_mutex);
                        _finished_queries.emplace(query_number);
                    }
                    notifyForResults(query_object);
                }
            }
            else {
                char reply = char(GET_NEXT_RESULT_SYMBOL);
                LOG_INFO("Server sends back 'lastResult' for query " << query_number)
                _infiniband.send_message(q_num, msg_id, &reply, sizeof(reply));
                {
                    // delete query objects from all relevant data structures
                    std::lock_guard<std::mutex> lock(_finished_queries_mutex);
                    _finished_queries.erase(query_number);
                }
                {
                    std::lock_guard<std::mutex> lock(_query_references_mutex);
                    _query_references.erase(query_number);
                }
            }
        }
}

/**
 * @brief InfinibandRTACommunication::run
 * this main thread is responsible for receiving messages and sending messages.
 * If the number of communication threads is greater one, additional threads (repsonsible
 * for sending back result packages) are created
 */
void
InfinibandRTACommunication::run()
{

    // start communication helper threads
    std::vector<std::thread> helper_threads;
    helper_threads.reserve(_communication_threads-1);
    for (uint8_t i = 0; i < (_communication_threads-1); ++i) {
        helper_threads.emplace_back([&](){while(_is_running){_sendResults();}});
    }

    auto process_request = [&] (uint32_t qp_num, uint32_t msg_id, char* buf, size_t size)
    {
            vector<char> additional_args(2*sizeof(uint32_t));
            *reinterpret_cast<uint32_t*>(additional_args.data()) = qp_num;
            *reinterpret_cast<uint32_t*>(&additional_args[sizeof(uint32_t)]) = msg_id;

            uint32_t query_type = *reinterpret_cast<const uint32_t*>(buf);
            if (query_type == GET_NEXT_RESULT_SYMBOL) {
                // client asks for next result --> update qp_num and msgId
                uint32_t query_number = *reinterpret_cast<const uint32_t*>(&buf[sizeof(uint32_t)]);

                {
                    std::lock_guard<std::mutex> lock(_active_queries_mutex);
                    _query_references[query_number]->setAdditionalArgs(std::move(additional_args));
                }
                LOG_INFO ("Server got getNext request for query " << query_number);
            }
            else {
                // client asks new query --> create it and answer queryNum
                auto query_object = _createQueryObject(buf, std::vector<char>());
                AbstractQueryServerObject* queryObject_ptr = query_object.get();
                {
                    std::lock_guard<std::mutex> lock(_query_references_mutex);
                    _query_references.emplace(queryObject_ptr->getQueryNr(), std::move(query_object));
                }
                {
                    std::lock_guard<std::mutex> lock(_active_queries_mutex);
                    _active_queries.emplace_back(queryObject_ptr);
                }
                uint32_t reply = queryObject_ptr->getQueryNr();
                LOG_INFO ("Server sends back query number, which is "
                          << queryObject_ptr->getQueryNr());               
                _infiniband.send_message(qp_num, msg_id, reinterpret_cast<char*>(&reply), sizeof(reply));
            }
    };

    // interleave checking for arrived messages with checking for messages to be sent
    _infiniband.run([&](){_sendResults();}, process_request);

    // clean-up helper thread objects
    for (auto & helper_thread : helper_threads) {
        helper_thread.join();
    }
}
