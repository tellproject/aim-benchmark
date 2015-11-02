#include "event.h"

#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>

/*
 * It creates an event based on char* following a specific format displayed
 * below.
 */
Event::Event(const char* stringEvent)
{
    char buff[5];
    int ret = std::sscanf(stringEvent, "%lu|%lu|%lu|%lf|%u|%[a-z]|%lu|%lu|%llu",
                          &_callee_id, &_caller_id, &_call_id, &_cost, &_duration,
                          buff, &_callee_place, &_caller_place , &_timestamp);
    (void)ret;//fixes "unused variable" warning
#ifndef NDEBUG
    assert(ret==9);
#endif

    strcmp(buff, "true") ? _long_distance = false : _long_distance = true;
}

void
Event::print() const
{
    std::stringstream s;
    s << "call_id: " << _call_id << std::endl;
    s << "callee: " << _callee_id << std::endl;
    s << "caller: " << _caller_id << std::endl;
    s << "time: " << _timestamp << std::endl;
    s << "dur: " << _duration << std::endl;
    s << "cost: " << _cost << std::endl;
    s << "long_dist: " << _long_distance << std::endl;
    s << "place_caller: " << _caller_place << std::endl;
    s << "place_calee: " << _callee_place << std::endl;
    std::cout << s.str() << std::endl;
}

Event
Event::deserialize(const char *serializedString)
{
    Event result;
    result.setCallId(*reinterpret_cast<const ulong *>
                     (&serializedString[s_call_id_offset]));
    result.setCallerId(*reinterpret_cast<const ulong *>
                     (&serializedString[s_caller_id_offset]));
    result.setCalleeId(*reinterpret_cast<const ulong *>
                     (&serializedString[s_callee_id_offset]));
    result.setCost(*reinterpret_cast<const double *>
                     (&serializedString[s_cost_offset]));
    result.setDuration(*reinterpret_cast<const uint *>
                     (&serializedString[s_duration_offset]));
    result.setLongDistance(*reinterpret_cast<const bool *>
                     (&serializedString[s_long_distance_offset]));
    result.setPlaceCaller(*reinterpret_cast<const ulong *>
                     (&serializedString[s_caller_place_offset]));
    result.setPlaceCallee(*reinterpret_cast<const ulong *>
                     (&serializedString[s_callee_place_offset]));
    result.setTimestamp(*reinterpret_cast<const Timestamp *>
                     (&serializedString[s_timestamp_offset]));
    return result;
}

char*
Event::serialize()
{
    char * result = new char[s_serialized_event_size];
    *(reinterpret_cast<ulong *>(&result[s_call_id_offset])) = this->callId();
    *(reinterpret_cast<ulong *>(&result[s_caller_id_offset])) = this->callerId();
    *(reinterpret_cast<ulong *>(&result[s_callee_id_offset])) = this->calleeId();
    *(reinterpret_cast<double *>(&result[s_cost_offset])) = this->cost();
    *(reinterpret_cast<uint *>(&result[s_duration_offset])) = this->duration();
    *(reinterpret_cast<bool *>(&result[s_long_distance_offset])) = this->longDistance();
    *(reinterpret_cast<ulong *>(&result[s_caller_place_offset])) = this->placeCaller();
    *(reinterpret_cast<ulong *>(&result[s_callee_place_offset])) = this->placeCallee();
    *(reinterpret_cast<Timestamp *>(&result[s_timestamp_offset])) = this->timestamp();
    return result;
}
