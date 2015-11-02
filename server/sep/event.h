#pragma once

#include <cstdint>
#include <iostream>

#include "sep/utils.h"

/*
 * LB: added methods for serialization
 * Class Event: models an event in the system. It contains a number of fields
 * defined in the SEP benchmark.
 */
class Event
{
public:
    Event() = default;
    Event(const char *stringEvent);
    ~Event() = default;

public:
    /*
     * These methods are used to send events over the network
     */
    static Event deserialize(const char *serializedString);
    char * serialize();
    static constexpr size_t s_serialized_event_size =
            5 * sizeof(ulong) +
            sizeof(double) +
            sizeof(uint) +
            sizeof(bool) +
            sizeof(Timestamp);

public:
    /*
     * Setters
     */
    ulong callId() const;
    ulong callerId() const;
    ulong calleeId() const;
    double cost() const;
    uint duration() const;
    bool longDistance() const;
    ulong placeCaller() const;
    ulong placeCallee() const;
    Timestamp timestamp() const;


public:
    /*
     * Getters
     */
    void print() const;
    void setCallId(ulong);
    void setCallerId(ulong);
    void setCalleeId(ulong);
    void setCost(double);
    void setDuration(uint);
    void setLongDistance(bool);
    void setPlaceCaller(ulong);
    void setPlaceCallee(ulong);
    void setTimestamp(Timestamp);


private:
    ulong _call_id = 0;
    ulong _caller_id = 0;
    ulong _callee_id = 0;
    double _cost = 0;
    uint _duration = 0;
    bool _long_distance = false;
    ulong _caller_place = 0;
    ulong _callee_place = 0;
    Timestamp _timestamp = 0;

private:
    /*
     * used for serialization
     */
    static constexpr uint8_t s_call_id_offset = 0;
    static constexpr uint8_t s_caller_id_offset = s_call_id_offset + sizeof(ulong);
    static constexpr uint8_t s_callee_id_offset = s_caller_id_offset + sizeof(ulong);
    static constexpr uint8_t s_cost_offset = s_callee_id_offset + sizeof(ulong);
    static constexpr uint8_t s_duration_offset = s_cost_offset + sizeof(double);
    static constexpr uint8_t s_long_distance_offset = s_duration_offset + sizeof(uint);
    static constexpr uint8_t s_caller_place_offset = s_long_distance_offset + sizeof(bool);
    static constexpr uint8_t s_callee_place_offset = s_caller_place_offset + sizeof(ulong);
    static constexpr uint8_t s_timestamp_offset = s_callee_place_offset + sizeof(ulong);
};

inline ulong
Event::callId() const
{
    return _call_id;
}

inline ulong
Event::callerId() const
{
    return _caller_id;
}

inline ulong
Event::calleeId() const
{
    return _callee_id;
}

inline double
Event::cost() const
{
    return _cost;
}

inline uint
Event::duration() const
{
    return _duration;
}

inline bool
Event::longDistance() const
{
    return _long_distance;
}

inline ulong
Event::placeCaller() const
{
    return _caller_place;
}

inline ulong
Event::placeCallee() const
{
    return _callee_place;
}

inline Timestamp
Event::timestamp() const
{
    return _timestamp;
}

inline void
Event::setCallId(ulong call_id)
{
    _call_id = call_id;
}

inline void
Event::setCallerId(ulong caller_id)
{
    _caller_id = caller_id;
}

inline void
Event::setCalleeId(ulong callee_id)
{
    _callee_id = callee_id;
}

inline void
Event::setCost(double cost)
{
    _cost = cost;
}

inline void
Event::setDuration(uint duration)
{
    _duration = duration;
}

inline void
Event::setLongDistance(bool long_distance)
{
    _long_distance = long_distance;
}

inline void
Event::setPlaceCaller(ulong caller_place)
{
    _caller_place = caller_place;
}

inline void
Event::setPlaceCallee(ulong callee_place)
{
    _callee_place = callee_place;
}

inline void
Event::setTimestamp(Timestamp timestamp)
{
    _timestamp = timestamp;
}
