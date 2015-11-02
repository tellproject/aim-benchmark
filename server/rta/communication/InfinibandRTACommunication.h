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

#include <unordered_map>
#include <unordered_set>
#include "rta/communication/AbstractRTACommunication.h"
#include "util/infiniband/inf_server.h"
#include "util/system-constants.h"

/**
 * @brief The InfinibandRTACommunication class implements the RTA communication
 * using Infiniband communication. The communication threads are optimized in the
 * way that as long there are messages to be delivered to a client that is ready,
 * progress is always made.
 */
class InfinibandRTACommunication : public AbstractRTACommunication
{
public:
    InfinibandRTACommunication(uint32_t port_num,
                               uint8_t scan_threads,
                               uint8_t communication_threads,
                               const AIMSchema& aim_schema,
                               const DimensionSchema& dim_schema);

    ~InfinibandRTACommunication();

protected:
    void run();

private:
    /**
     * @brief _infiniband
     * infiniband instance (singleton object)
     */
    util::inf_server _infiniband;

    /**
     * @brief _mainThread
     * handle for the main thread. The main thread is created and started
     * automatically by the constructor of the class.
     */
    std::thread _main_thread;

    /**
     * @brief _queryReferences
     * a map from query numbers to query references
     */
    std::unordered_map<uint32_t, std::unique_ptr<AbstractQueryServerObject> > _query_references;
    std::mutex _query_references_mutex;

    /**
     * @brief _finishedQueries
     * set of numbers of finshed queries (all results have been delivered,
     * but the last-result-message is still pending)
     */
    std::unordered_set<uint32_t> _finished_queries;

    /**
     * @brief _finishedQueriesMutes
     * used to synchronize access on finished queries
     */
    std::mutex _finished_queries_mutex;

private:
    /**
     * @brief _sendResults
     * the method to send back results to the client
     */
    void _sendResults();
};
