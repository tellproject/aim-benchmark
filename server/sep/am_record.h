#pragma once

#include <inttypes.h>

#include "sep/aim_schema.h"
#include "sep/event.h"

/*
 * This class implements a record of a subscriber in the AM table. The actual
 * data is represented using a vector of characters.
 *
 * For creating an AM record we loop over the AM attributes and we invoke the
 * update attribute function.
 *
 * Sample Usage:    AMRecord init(schema, timestamp);
 *                  AMRecord updated(read_data, schema, event);
 */
class AMRecord
{
public:
    AMRecord() = default;
    AMRecord(const AIMSchema&, Timestamp start);
    AMRecord(const AIMSchema&, const Event&);
    AMRecord(const char*, const AIMSchema&, const Event&);
    AMRecord(const char*, size_t size);
    AMRecord& operator=(AMRecord&&) = default;
    AMRecord(AMRecord&&) = default;

public:
    const std::vector<char>& data() const { return _data; }

private:
    std::vector<char> _data;
};
