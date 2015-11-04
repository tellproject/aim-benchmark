/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
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
    Timestamp ts = e.timestamp;       //write timestamp into the
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
