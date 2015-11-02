#pragma once

#include <cstdint>

#include "sep/event.h"
#include "util/InitializerArray.h"
#include "util/folly/ProducerConsumerQueue.h"
#include "util/system-constants.h"

/**
 * @brief The AbstractSEPCommunication class
 * is responsible for receiving events that come over the network. Events are
 * stored in queues depending on the hash_to_queue_id function. Classes that
 * implement the AbstractSEPCommunication class are responisible for filling
 * the queues.
 */
class AbstractSEPCommunication
{
public:
    AbstractSEPCommunication(uint32_t port);
    virtual ~AbstractSEPCommunication() {}

public:
    typedef folly::ProducerConsumerQueue<Event> EventQueue;

    // pointer to the value at the front of the queue (for use in-place) or
    // nullptr if empty.
    Event* frontPtr(uint8_t queue_id)
    {
        return _queues[queue_id].frontPtr();
    }

    void popFront(uint8_t queue_id)
    {
        _queues[queue_id].popFront();
    }

public://for testing purposes
    void __queueEvent(int queue_id, const Event& event)
    {
        while (!_queues[queue_id].write(event));
    }

    size_t queuesSize() const { return _queues.size(); }

protected:
    uint32_t _port;
    InitializerArray<EventQueue, SEP_THREAD_NUM> _queues;
};
