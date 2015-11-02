#include "conjunct.h"

/*
 * Constructor: _predicates is unique_ptr and for this reason we need to use
 * reset for changing its value. If preds is empty we have a null pointer as
 * the value of _predicates but this is not a problem, because _predicate_num
 * equals to 0.
 */
Conjunct::Conjunct(uint16_t camp_id, std::vector<Predicate*> preds):
    _campaign_id(camp_id), _predicate_num(preds.size())
{
    _predicates.reset(_predicate_num ? new Predicate*[_predicate_num] : nullptr);
    for (size_t i=0; i<_predicate_num; ++i)
        _predicates.get()[i] = preds.at(i);
}

void
Conjunct::setCampaignId(uint16_t camp_id)
{
    _campaign_id = camp_id;
}

/*
 * It builds an array of pointers to predicates given a vector of pointers to
 * predicates. Given that we use unique_ptr for the predicates array we reset
 * it in order to assign a new value to it.
 */
void
Conjunct::setPredicates(std::vector<Predicate*> preds)
{
    _predicate_num = preds.size();
    _predicates.reset(_predicate_num ? new Predicate*[_predicate_num] : nullptr);
    for (size_t i=0; i<preds.size(); ++i)
        _predicates.get()[i] = preds.at(i);
}
