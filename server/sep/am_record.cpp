#include "am_record.h"

#include <cassert>
#include <cstring>
#include <iostream>

/*
 * We always place the timestamp of the last event of every subscriber at the
 * beggining of his/her AM record. This way we can calculate the active window
 * for each AM attribute.
 */
uint16_t
writeTimestamp(char** tmp, const Event& e)
{
    Timestamp ts = e.timestamp();       //write timestamp into the
    memcpy(*tmp, &ts, sizeof(ts));      //beginning of the full record
    *tmp += sizeof(ts);
    return (uint16_t)sizeof(ts);
}

/*
 * Writes given timestamp to the beggining of the AM record. It is only invoked
 * for the population of the database. We use a default value for the timestamp.
 */
uint16_t
writeTimestamp(char** tmp, Timestamp ts)
{
    memcpy(*tmp, &ts, sizeof(ts));
    *tmp += sizeof(ts);
    return (uint16_t)sizeof(ts);
}

/*
 * Creates a dummy record by calling the default init function for all the AM
 * attributes. It is called only for the population of the db. It loops over
 * the existing attributes in the AM table, and for each of them it initializes
 * it's value to a default value, e.g. Sum of calls today = 0.
 */
AMRecord::AMRecord(const AIMSchema& schema, Timestamp start)
{
    _data.resize(schema.size());
    memset(_data.data(), '0', schema.size());

    char* tmp = _data.data();
    uint16_t written = writeTimestamp(&tmp, start);
    for (size_t i=0; i<schema.numOfEntries(); ++i) {
        uint16_t cur = schema[i].initDef(&tmp);
        written = (uint16_t)(written + cur);
    }

#ifndef NDEBUG
    assert(written == schema.size());
#endif
}

/*
 * Creates a record for a subsriber based on the event information. This event
 * is the very first event of the underline subscriber, and that is why we do
 * not have previous data to use. Based on the outcome of the filter function
 * we either invoke the init function, or the init default function. The filter
 * function checks locality information for an event.
 * E.g. attribute: "num of local calls this week", then the filter checks whether
 * an incoming event corresponds to a local call. If the event is a local one, we
 * simply assign the proper value to the AM attribute.
 * E.g. Cost today = event.cost. Otherwise, we use the default initialization.
 */
AMRecord::AMRecord(const AIMSchema& schema, const Event& event)
{
    _data.resize(schema.size());
    memset(_data.data(), '0', schema.size());
    char *tmp = _data.data();

    uint16_t written = writeTimestamp(&tmp, event);
    uint16_t cur = 0;
    for (size_t i=0; i<schema.numOfEntries(); ++i) {
        const auto& entry = schema[i];
        if (entry.filter(event))
            cur = entry.init(&tmp, event);
        else
            cur = entry.initDef(&tmp);

        written = (uint16_t)(written + cur);
    }

#ifndef NDEBUG
    assert(written == schema.size());
#endif
}

/*
 * Updates an already existing AM record. For doing so, it uses the previous
 * record of this subscriber and current event information. For every AM
 * attribute, we first apply the filter function and based on the result we
 * either update or maintain a value.
 */
AMRecord::AMRecord(const char* r_data, const AIMSchema& schema, const Event& event)
{
    _data.resize(schema.size());
    memset(_data.data(), '0', schema.size());
    char* tmp = _data.data();
    const char *r_tmp = r_data;

    Timestamp old_ts;
    memcpy(&old_ts, r_tmp, sizeof(old_ts));
    r_tmp += sizeof(old_ts);

    uint16_t written = writeTimestamp(&tmp, event);
    uint16_t cur = 0;
    for (size_t i=0; i<schema.numOfEntries(); ++i) {
        const auto& entry = schema[i];
        if (entry.filter(event))
            cur = entry.update(&tmp, &r_tmp, entry, old_ts, event);
        else
            cur = entry.maintain(&tmp, &r_tmp, entry, old_ts, event);

        written = (uint16_t)(written + cur);
    }

#ifndef NDEBUG
    assert(written == schema.size());
#endif
}
