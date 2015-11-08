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
#include <telldb/Transaction.hpp>
#include <common/Protocol.hpp>
#include <common/Util.hpp>

#include "CreateSchema.hpp"

#include "server/sep/aim_schema.h"

namespace aim {

struct Context;

class Transactions {

public:

    /**
     * takes a transaction in the constructor such that schema can be obained at startup time
     */
    Transactions(const AIMSchema aimSchema):
            mAimSchema(aimSchema)
    {}

    void processEvent(tell::db::Transaction& tx, Context &context, std::vector<Event> &events);

    Q1Out q1Transaction(tell::db::Transaction& tx, Context &context, const Q1In& in);
    Q2Out q2Transaction(tell::db::Transaction& tx, Context &context, const Q2In& in);
    Q3Out q3Transaction(tell::db::Transaction& tx, Context &context);
    Q4Out q4Transaction(tell::db::Transaction& tx, Context &context, const Q4In& in);
    Q5Out q5Transaction(tell::db::Transaction& tx, Context &context, const Q5In& in);
    Q6Out q6Transaction(tell::db::Transaction& tx, Context &context, const Q6In& in);
    Q7Out q7Transaction(tell::db::Transaction& tx, Context &context, const Q7In& in);

private:
    const AIMSchema &mAimSchema;

};

} // namespace aim

