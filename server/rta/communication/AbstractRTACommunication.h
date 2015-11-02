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

#include <cstdint>
#include <thread>
#include <atomic>
#include <mutex>
#include <queue>
#include <vector>

//forward declaration
class AbstractQueryServerObject;

#include "query/AbstractQueryServerObject.h"
#include "rta/dimension_schema.h"
#include "sep/aim_schema.h"
#include "util/system-constants.h"

/**
 * @brief The AbstractServerCommunication class
 * This class is used on the server side in order to communicate to the client.
 * It keeps track of pending queries. Once, the function getActiveQueries(.)
 * (e.g. by a scan-thread) is called, the pending queries are returned and a
 * new list of pending queries is created. If query objects have (partial)
 * results available, they can signal the communication object using
 * notifyForResults. Upon creation, the communication module runs its own
 * threads, which means there is one thread that listens for new requests
 * and a number of helper threads (defined by numberOfCommunicaitonThreads) that
 * deliver results back to the clients. numberOfScanThreads is used to
 * create new query objects (which have to know this in order to know when
 * processing is finished).
 * In order to make the AbstractServerCommunication object stop working, one
 * can simply call its destructor which will make sure that all threads
 * are stopped and de-allocated correctly.
 */
class AbstractRTACommunication
{
public:

    /**
     * @brief AbstractServerCommunication
     * constructs a new server communication module with the given parameters.
     * @param portNumber
     * @param numberOfScanThreads
     * @param numberOfCommunicationThreads
     */
    AbstractRTACommunication(uint32_t port_num,
                             uint8_t scan_threads,
                             uint8_t communication_threads,
                             const AIMSchema& aim_schema,
                             const DimensionSchema& dim_schema);

    /**
     * @brief ~AbstractServerCommunication
     * make sure that all threads of the derived class are joined.
     */
    virtual ~AbstractRTACommunication();

public:
    /**
     * @brief notifyForResults
     * This method is used by query ojbects in order to notify that it can be
     * polled for results. It is important that this method is short and that
     * it does not do the actual polling (which should be done asychronously).
     * @param queryObject
     * @param queryNumber
     */
    virtual void notifyForResults(AbstractQueryServerObject* queryObject);

    /**
     * @brief getActiveQueries
     * returns a new vector of the active queries (i.e. the next queries to
     * be processed). Causes the server communication to start a new batch
     * of queries and should therefore only be called once per query scanning
     * batch (and not one for every scan thread).
     * @return a reference to the vector of active queries.
     */
    std::vector<AbstractQueryServerObject*> getQueuedQueries();

public://for testing purposes
    void __enqueueQuery(AbstractQueryServerObject* query_object) {
         std::lock_guard<std::mutex> lock(_active_queries_mutex);
        _active_queries.emplace_back(query_object);
    }

protected:
    const AIMSchema& _aim_schema;
    const DimensionSchema& _dim_schema;
    /**
     * @brief m_portNumber
     * port on which the communication server should listen for connections
     */
    const uint32_t _port_num;

    /**
     * @brief m_numberOfScanThreads
     * number of threads that will work on the query objects in parallel and
     * call end(.) in the end.
     */
    const uint8_t _scan_threads;

    /**
     * @brief m_numberOfCommunicationThreads
     * number of threads that communicate results back to client (should be
     * somehow balanced with the number of scan threads.
     */
    const uint8_t _communication_threads;

    /**
     * @brief m_isRunning
     * attribute that is true while the threads are running
     */
    bool _is_running;

    /**
     * @brief m_activeQueries
     * container for queries that arrive to the system
     */
    std::vector<AbstractQueryServerObject*> _active_queries;

protected:

    /**
     * @brief createQueryObject
     * create a query object from the message string
     * @param message message string coming from client
     * structure is uint32_t QueryTypeNumber | [additional params]
     * @param additionalArguments that are passed to the callback function
     *
     * @return an abstract query object that corresponds to the message-string
     */
    std::unique_ptr<AbstractQueryServerObject> _createQueryObject(char *msg,
            std::vector<char> additional_args);

    /**
     * @brief getNextQueryNumber returns the next query number that can be used
     * for a newly registered query.
     * @return a (server-wide) unique query number
     */
    uint32_t _getNextQueryNumber();

    /**
     * @brief getNextQueryObjectToPoll
     * returns the next query object to poll for results. This is used by the
     * worker threads to find out which object to poll next. The method is non-
     * blocking which means it returns a nullptr when the queue is empty.
     * @return a reference to a query object that has results available or a
     * null pointer if there is no such object.
     */
    AbstractQueryServerObject* _getNextQueryObject();

private:
    /**
     * @brief queryCounter
     * This counter is used to assign unique query-ids for queries.
     */
    std::atomic<uint32_t> _query_counter;

    /**
     * @brief m_pollingQueue
     * This FIFO-queue is used to get/set the next query object to poll.
     */
    std::queue<AbstractQueryServerObject*> _polling_queue;

protected:
    /**
     * @brief m_queueMutex
     * used to synchronize access on queue
     */
    std::mutex _queue_mutex;

    /**
     * @brief m_activeQueriesMutex
     * used to synchronize access on active queries
     */
    std::mutex _active_queries_mutex;
};

