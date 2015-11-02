#ifndef BLOCKINGCONSUMERQUEUE_H
#define BLOCKINGCONSUMERQUEUE_H

#include <mutex>
#include <thread>
#include "logger.h"
#include "system-constants.h"

/**
 * This class implements a FIFO queue with elements of type T with the following
 * characteristics:
 * 1. The queue is thread-safe.
 * 2. The queue is blocking in the sense that pop or push block if the queue is
 * empty, resp. full.
 * 3. The queue uses move-semantics, which means objects are not copied, but
 * moved inside and outside the queue.
 * 4. A good mode to operat this queue is to have exactly one producer and
 * one consumer thread.
 *
 * NOTE that the queue has a fixed capacity, which can easily be changed by
 * adding another constructor BlockingConsumerQueue(uint capacity).
 */
template<typename T>
class BlockingConsumerQueue {

public:

    BlockingConsumerQueue();
    virtual ~BlockingConsumerQueue();

public:

    /**
     * @brief push
     * pushes an element at the end of the queue. Note the rvalue reference!
     * blocks until queue is non-full
     * @param element
     */
    void push(T && element);

    /**
     * @brief pop
     * retrieves and deletes the front of the queue. Note the rvalue reference!
     * blocks until queue is non-empty
     * @return
     */
    T pop();


private:

    T _values [SEP_SERVER_QUEUES_SIZE];
    uint _capacity;
    uint _head; // points to the first element in the queue
    uint _tail; // points to the last element in the queue
    std::mutex _queue_mutex;
};

template<typename T>
BlockingConsumerQueue<T>::BlockingConsumerQueue() :
    _capacity(SEP_SERVER_QUEUES_SIZE),
    _head(0),
    _tail(0),
    _queue_mutex()
{

}

template<typename T>
BlockingConsumerQueue<T>::~BlockingConsumerQueue() {
    delete [] _values;
}

template<typename T>
void BlockingConsumerQueue<T>::push(T && element) {
    while (true) {
        {
            std::lock_guard<std::mutex> lock(_queue_mutex);
            if (((_head - _tail + _capacity) % _capacity) == 1) {
                // queue full
                LOG_WARNING("Consumer queue full, has to wait")
            } else {
                // move value in
                _tail = (_tail + 1) % _capacity;
                _values[_tail] = std::move(element);
                return;
            }
        }
        std::this_thread::yield();
    }
}

template<typename T>
T BlockingConsumerQueue<T>::pop() {
    while (true) {
        {
            std::lock_guard<std::mutex> lock(_queue_mutex);
            if (_head != _tail)  {
                T result = std::move(_values[_head]);
                _head = (_head + 1) % _capacity;
                return std::move(result);
            }
        }
        // queue empty
        std::this_thread::yield();
    }
}

#endif // BLOCKINGCONSUMERQUEUE_H
