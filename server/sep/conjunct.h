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

#include <cassert>
#include <memory>

#include "sep/am_record.h"
#include "sep/predicate.h"

/*
 * This class implements the logic of Conjunct. It contains an array of
 * pointers to the actual predicates (global representation of predicates). It
 * also houses the id of the campaign this conjucnt belongs to.
 *
 * Sample Usage: Conjunct c(arguments)
 *               AMRecord record(arguments);
 *               bool result = c.evaluate(record);
 */
class Conjunct
{
public:
    Conjunct() = default;
    Conjunct(uint16_t camp_id, std::vector<Predicate*> preds);
    Conjunct(Conjunct &&) = default;
    Conjunct & operator=(Conjunct &&) = default;

public:
    bool evaluate(const AMRecord &record) const;
    uint16_t campaignId() const;
    void setCampaignId(uint16_t);
    void setPredicates(std::vector<Predicate*>);

private:
    uint16_t _campaign_id = 0;
    std::unique_ptr<Predicate *[]>_predicates;
    size_t _predicate_num = 0;
};

/*
 * We use the inline keyword to take advantage of the optimizer and insert the
 * function's code into the caller's code. We evaluate all predicates belonging
 * to a conjunct sequentially. If we encounter a false predicate conjunct
 * evaluation aborts. E.g. Conjunct: "a AND b AND c", where a, b, c predicates.
 * If one of these three predicates is false then the whole conjunct results in
 * false.
 */
inline bool
Conjunct::evaluate(const AMRecord &record) const
{
    for (size_t i=0; i<_predicate_num; ++i)
        if (!_predicates.get()[i]->evaluate(record))
            return false;
    return true;
}

inline uint16_t
Conjunct::campaignId() const
{
    return _campaign_id;
}
