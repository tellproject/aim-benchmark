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
