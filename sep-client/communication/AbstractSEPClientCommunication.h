#pragma once

#include <atomic>
#include <cstdint>
#include <random>
#include <vector>

#include "server/sep/event.h"
#include "util/system-constants.h"

/**
 * @brief The AbstractSEPClientCommunication class
 * The client starts its communication upon creation. Communication can be
 * stopped by calling stopSending(.). The main thread does not stop immediately,
 * but waits for the current event to be sent. One can wait for this to happen
 * by calling waitFor(.). Finally, getSentEvents(.) returns the total number
 * of events that was sent during this period.
 */
class AbstractSEPClientCommunication
{
public:
    /**
     * @brief AbstractSEPClientCommunication
     * @param server_addresses
     * @param wait_time wait time in micro-seconds
     */
    AbstractSEPClientCommunication(const std::vector<ServerAddress>&,
                                   ulong wait_time,
                                   uint thread_id);

    virtual ~AbstractSEPClientCommunication();

public:
    ulong getSentEvents();

    /**
     * @brief stopSending
     * can be called to stop the main thread. After this call, the only thing
     * which can be done is retrieving the number of sent events.
     */
    void stopSending();

    /**
     * @brief waitFor
     * can be called after stopSending() to wait for the main thread to have
     * ended.
     */
    virtual void waitFor() = 0;

protected:
    /**
     * @brief _server_addresses
     * addresses of the different servers. Addresses consist of host and port.
     */
    std::vector<ServerAddress> _server_addresses;

    /**
     * @brief _numberOfServers
     * number of query servers
     */
    uint32_t _number_of_servers;

    /**
     * @brief _wait_time
     */
    long _wait_time;

    /**
     * @brief _is_running
     * main thread is running
     */
    bool _is_running;

    /**
     * @brief _sent_events
     * counter for sent events
     */
    ulong _sent_events;

    /**
     * @brief _send_buf
     * we keep one send buffer per server
     */
    char _send_buf [Event::s_serialized_event_size];

    /**
     * @brief _thread_id
     */
    uint _thread_id;

protected:
    virtual void _send_event_to_server(uint32_t server_id) = 0;
    void run();

private :
    /**
     * @brief Engine
     * random engine
     */
    typedef std::mt19937 Engine;    //TODO:this engine is slow
    // but as client has not much work, no problem...
    Engine _engine;

    typedef std::uniform_real_distribution<double> DoubleDistr;
    typedef std::uniform_int_distribution<ulong> LongDistr;
    typedef std::uniform_int_distribution<uint> IntDistr;

    DoubleDistr _double_distr;   //cost
    IntDistr _uint_distr;          //duration
    LongDistr _ulong_distr;
    IntDistr _bool_distr;              //is_long_distance


private:
    Event _create_random_event();
    inline uint32_t _get_server_Id(Event & e);

};
