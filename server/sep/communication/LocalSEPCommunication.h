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
