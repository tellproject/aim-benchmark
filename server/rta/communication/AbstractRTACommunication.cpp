#include "AbstractRTACommunication.h"

#include <cstdio>
#include <iostream>
#include <unordered_set>

#include "query/Query1ServerObject.h"
#include "query/Query2ServerObject.h"
#include "query/Query3ServerObject.h"
#include "query/Query4ServerObject.h"
#include "query/Query5ServerObject.h"
#include "query/Query6ServerObject.h"
#include "query/Query7ServerObject.h"
#include "util/logger.h"

AbstractRTACommunication::AbstractRTACommunication(uint32_t port_num,
                                                   uint8_t scan_threads,
                                                   uint8_t communication_threads,
                                                   const AIMSchema& aim_schema,
                                                   const DimensionSchema& dim_schema):
    _aim_schema(aim_schema),
    _dim_schema(dim_schema),
    _port_num(port_num),
    _scan_threads(scan_threads),
    _communication_threads(communication_threads),
    _is_running(true),
    _query_counter(0)
{}

AbstractRTACommunication::~AbstractRTACommunication()
{
    // delete objects in activeQueries
    for (auto& query: _active_queries) {
        delete query;
    }

    // delete pollingQueue (by making sure no reference is deleted twice)
    std::unordered_set<AbstractQueryServerObject *> queries_to_delete;
    while (!_polling_queue.empty()) {
        queries_to_delete.emplace(_polling_queue.front());
        _polling_queue.pop();
    }
    for (auto &query: queries_to_delete) {
        delete query;
    }
}

void
AbstractRTACommunication::notifyForResults(AbstractQueryServerObject* queryObject)
{
    LOG_INFO("Server was notified for results about query "
             << queryObject->getQueryNr());

    // thread-safe operation
    std::lock_guard<std::mutex> lock(_queue_mutex);
    _polling_queue.emplace(queryObject);
}

std::vector<AbstractQueryServerObject*>
AbstractRTACommunication::getQueuedQueries()
{
    if (_active_queries.empty())
        return decltype(_active_queries)();

    std::lock_guard<std::mutex> lock(_active_queries_mutex);
    return std::move(_active_queries);
}

std::unique_ptr<AbstractQueryServerObject>
AbstractRTACommunication::_createQueryObject(char *msg,
                                            std::vector<char> additional_args)
{
    auto query_type = *reinterpret_cast<uint32_t*>(msg);
    switch (query_type) {
    case 1:
    {
        uint32_t alpha = *reinterpret_cast<uint32_t*>(&msg[sizeof(uint32_t)]);
        LOG_INFO("Server creates new Q1 object with alpha = " << alpha);
        return std::unique_ptr<AbstractQueryServerObject>(
                    new Query1ServerObject(_getNextQueryNumber(),
                                           _scan_threads, this,
                                           std::move(additional_args),
                                           _aim_schema, alpha));
    }
    case 2:
    {
        uint32_t alpha = *reinterpret_cast<uint32_t*>(&msg[sizeof(uint32_t)]);
        LOG_INFO("Server creates new Q2 object with alpha = " << alpha);
        return std::unique_ptr<AbstractQueryServerObject>(
                    new Query2ServerObject(_getNextQueryNumber(),
                                           _scan_threads, this,
                                           std::move(additional_args),
                                           _aim_schema, alpha));
    }
    case 3:
    {
        LOG_INFO("Server creates new Q3 object");
        return std::unique_ptr<AbstractQueryServerObject>(
                    new Query3ServerObject(_getNextQueryNumber(),
                                           _scan_threads, this,
                                           std::move(additional_args), _aim_schema));
    }
    case 4:
    {
        uint32_t alpha = *reinterpret_cast<uint32_t*>(&msg[sizeof(uint32_t)]);
        uint32_t beta = *reinterpret_cast<uint32_t*>(&msg[2*sizeof(uint32_t)]);
        LOG_INFO("Server creates new Q4 object with alpha = " << alpha <<
                 " and beta = " << beta);
        return std::unique_ptr<AbstractQueryServerObject>(
                    new Query4ServerObject(_getNextQueryNumber(),
                                           _scan_threads, this,
                                           std::move(additional_args),
                                           _aim_schema, _dim_schema,
                                           alpha, beta));
    }
    case 5:
    {
        uint16_t alpha = *reinterpret_cast<uint16_t*>(&msg[sizeof(uint32_t)]);
        uint16_t beta = *reinterpret_cast<uint16_t*>(&msg[sizeof(uint32_t) + sizeof(alpha)]);
        LOG_INFO("Server creates new Q5 object with alpha = " << alpha <<
                 " and beta = " << beta);
        return std::unique_ptr<AbstractQueryServerObject>(
                    new Query5ServerObject(_getNextQueryNumber(),
                                           _scan_threads, this,
                                           std::move(additional_args),
                                           _aim_schema, _dim_schema,
                                           alpha, beta));
    }
    case 6:
    {
        uint16_t alpha = *reinterpret_cast<uint16_t*>(&msg[sizeof(uint32_t)]);
        LOG_INFO("Server creates new Q6 object with alpha = " << alpha);
        return std::unique_ptr<AbstractQueryServerObject>(
                    new Query6ServerObject(_getNextQueryNumber(),
                                           _scan_threads, this,
                                           std::move(additional_args),
                                           _aim_schema, _dim_schema,
                                           alpha));
    }
    case 7:
    {
        uint16_t alpha = *reinterpret_cast<uint16_t*>(&msg[sizeof(uint32_t)]);
        uint16_t beta = *reinterpret_cast<uint16_t*>(&msg[sizeof(uint32_t) + sizeof(alpha)]);
        LOG_INFO("Server creates new Q7 object with alpha = " << alpha);
        return std::unique_ptr<AbstractQueryServerObject>(
                    new Query7ServerObject(_getNextQueryNumber(),
                                           _scan_threads, this,
                                           std::move(additional_args),
                                           _aim_schema, _dim_schema,
                                           WindowLength(alpha), beta));
    }
    default:
        assert(false);
        return nullptr;
    }
}

uint32_t
AbstractRTACommunication::_getNextQueryNumber()
{
    return _query_counter.fetch_add(1);
}

AbstractQueryServerObject*
AbstractRTACommunication::_getNextQueryObject()
{
    // thread-safe operation
    std::lock_guard<std::mutex> lock(_queue_mutex);
    if (_polling_queue.empty()) {
        return nullptr;
    }
    else {
        AbstractQueryServerObject* result = _polling_queue.front();
        _polling_queue.pop();
        return result;
    }
}
