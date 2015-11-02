#include "campaign_index.h"

#include <cassert>
#include <iostream>

CampaignIndex::CampaignIndex(Predicate *predicates, size_t pred_num,
                             Campaign *campaigns, size_t campaign_num,
                             Conjunct *unindexed, size_t unindexed_num,
                             std::vector<ConjVec>&& indexed,
                             EntryAttrIndexInterface **entry_indexes,
                             size_t entry_index_num)
{
    _predicates = predicates;
    _pred_num = pred_num;
    _unindexed = unindexed;
    _unindexed_num = unindexed_num;
    _campaigns = campaigns;
    _campaign_num = campaign_num;
    _indexed = std::move(indexed);
    _entry_indexes = entry_indexes;
    _entry_index_num = entry_index_num;
}

/*
 * Move constructor
 */
CampaignIndex::CampaignIndex(CampaignIndex &&other)
{
    _predicates = other._predicates;
    _pred_num = other._pred_num;
    _unindexed = other._unindexed;
    _unindexed_num = other._unindexed_num;
    _campaigns = other._campaigns;
    _campaign_num = other._campaign_num;
    _indexed = std::move(other._indexed);
    _entry_indexes = other._entry_indexes;
    _entry_index_num = other._entry_index_num;

    other._predicates = nullptr;
    other._pred_num = 0;
    other._unindexed = nullptr;
    other._unindexed_num = 0;
    other._campaigns = nullptr;
    other._campaign_num = 0;
    other._entry_indexes = nullptr;
    other._entry_index_num = 0;
}

/*
 * Move assignment operator
 */
CampaignIndex&
CampaignIndex::operator=(CampaignIndex &&other)
{
    if (this != &other) {
        for (size_t i=0; i<_entry_index_num; ++i) {     //set *this to valid state
            delete _entry_indexes[i];                   //before continuing
        }
        delete [] _entry_indexes;
        delete [] _predicates;
        delete [] _unindexed;
        delete [] _campaigns;

        _predicates = other._predicates;                //*this is in a valid state
        _pred_num = other._pred_num;
        _unindexed = other._unindexed;
        _unindexed_num = other._unindexed_num;
        _campaigns = other._campaigns;
        _campaign_num = other._campaign_num;
        _indexed = std::move(other._indexed);
        _entry_indexes = other._entry_indexes;
        _entry_index_num = other._entry_index_num;

        other._predicates = nullptr;
        other._pred_num = 0;
        other._unindexed = nullptr;
        other._unindexed_num = 0;
        other._campaigns = nullptr;
        other._campaign_num = 0;
        other._entry_indexes = nullptr;
        other._entry_index_num = 0;
    }
    return *this;
}

CampaignIndex::~CampaignIndex()
{
    for (size_t i=0; i<_entry_index_num; ++i) {
        delete _entry_indexes[i];
    }
    delete [] _entry_indexes;
    delete [] _predicates;
    delete [] _unindexed;
    delete [] _campaigns;
}

size_t
CampaignIndex::campaignNum() const
{
    return _campaign_num;
}

size_t
CampaignIndex::indexedNum() const
{
    return _entry_index_num;    
}

size_t
CampaignIndex::unindexedNum() const
{
    return _unindexed_num;    
}

void
CampaignIndex::evaluateUnindexed(Bitset &bitset, const AMRecord &record) const
{
    for (size_t i=0; i<_unindexed_num; ++i) {
        if(_unindexed[i].evaluate(record)) {
            bitset.set(_unindexed[i].campaignId());
        }
    }
}

/*
 * Evaluate conjunctions returned from an interval tree associated to an entry
 * attribute. Takes an array of std::vector<Conjunct>** and for every position
 * in this array evaluates the conjunctions taking part in this vector.
 */
void
CampaignIndex::evaluateConjuncts(Bitset &bitset, Element *conj_array,
                                 ushort array_size, const AMRecord &record) const
{
    for (ushort i=0; i<array_size; ++i) {      //for every position in the array
        for (Conjunct &c: *(conj_array[i])) { //for every conjunct
            if (c.evaluate(record)) {
                bitset.set(c.campaignId());
            }
        }
    }
}

/*
 * For every entry attribute we retrieve an array of vector<Conjunct>* elements
 * that correspond to keys (bounds) containing the value of the AM attribute this
 * index is built upon. Afterwards we evaluate all these elements. We also
 * evaluate the unindexed conjuncts.
 */
CampaignIndex::Bitset
CampaignIndex::matchCampaigns(const AMRecord &record) const
{
    Bitset bitset(_campaign_num);
    for (size_t i=0; i<_entry_index_num; ++i) {
        Element *found = nullptr;
        ushort noOfFound = _entry_indexes[i]->candConjuncts(record, &found);
        evaluateConjuncts(bitset, found, noOfFound, record);
        delete [] found;
    }
    evaluateUnindexed(bitset, record);
    return bitset;
}
