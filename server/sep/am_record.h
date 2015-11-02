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
