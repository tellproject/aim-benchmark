#include "sep-client/communication/AbstractSEPClientCommunication.h"

#include <thread>
#include <chrono>
#include <cstring>

#include "util/logger.h"
#include "util/system-constants.h"

AbstractSEPClientCommunication::AbstractSEPClientCommunication(
        const std::vector<ServerAddress>& addresses,
        ulong wait_time, uint thread_id):

    _server_addresses(addresses),
    _number_of_servers(addresses.size()),
    _wait_time(wait_time),
    _is_running(true),
    _sent_events(0),
    _thread_id(thread_id),
    _engine(thread_id),
    _double_distr(0.1, 100.0),
    _uint_distr(1, 10000),
    _ulong_distr(1, SUBSCR_NUM),
    _bool_distr(0, 1)
{}

AbstractSEPClientCommunication::~AbstractSEPClientCommunication()
{}

ulong
AbstractSEPClientCommunication::getSentEvents()
{
    return _sent_events;
}

void
AbstractSEPClientCommunication::stopSending()
{
    _is_running = false;
}

/*
 * Function responsible for generating an event based on the benchmark.
 */
Event
AbstractSEPClientCommunication::_create_random_event()
{
    Event e;
    // call-id is set to current time
    std::chrono::high_resolution_clock::time_point now =
            std::chrono::high_resolution_clock::now();
    Timestamp ts (std::chrono::duration_cast<std::chrono::seconds>
                  (now.time_since_epoch()).count());
    e.setCallId(_ulong_distr(_engine));
    e.setCalleeId(_ulong_distr(_engine));
    e.setCallerId(_ulong_distr(_engine));
    e.setTimestamp(ts);
    e.setDuration(_uint_distr(_engine));
    e.setCost(_double_distr(_engine));
    e.setLongDistance((_bool_distr(_engine) ? true : false));
    e.setPlaceCaller(_ulong_distr(_engine));
    e.setPlaceCallee(_ulong_distr(_engine));
    return e;
}

uint32_t
AbstractSEPClientCommunication::_get_server_Id(Event & e)
{
    return (e.callerId() >> 8) % _number_of_servers;
}

void
AbstractSEPClientCommunication::run()
{
    auto send_next = std::chrono::high_resolution_clock::now();
    while (_is_running) {
        Event e = _create_random_event();
        char * serialized = e.serialize();
        memcpy(_send_buf, serialized, Event::s_serialized_event_size);
        _send_event_to_server(_get_server_Id(e));
        LOG_INFO("Client sent event " << e.callId() << " with attribute " << e.callerId());
        _sent_events++;
        delete [] serialized;
        send_next += std::chrono::microseconds(_wait_time);
        std::this_thread::sleep_until(send_next);
    }
}
