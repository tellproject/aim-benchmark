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

#include <boost/dynamic_bitset.hpp>
#include <inttypes.h>

#include "sep/campaign.h"
#include "sep/entry_attr_index.h"
#include "sep/predicate.h"

/*
 * This class models the campaign index. It contains:
 * 1) the entry indices (interval trees),
 *    key: entry predicates, value: pointer to vector of conjuncts
 * 2) the unindexed conjunts (conjuncts containing non entry predicates)
 * 3) an array that stores the actual predicates
 * 4) an array keeping the campaigns
 * 5) a vector of vectors of conjuncts storing the values of the pointers we
 *    use as tree elements.
 *
 * Sample Usage: CampaignIndex c_i(arguments);
 *               AMRecord record(arguments);
 *               boost::dynamic_bitset<> *bitset = c_i.matchCampaigns(record);
 *               delete bitset;
 */
class CampaignIndex
{
public:
    typedef boost::dynamic_bitset<> Bitset;
    typedef std::vector<Conjunct>* Element;     //element we put into the tree
    typedef std::vector<Conjunct> ConjVec;

    CampaignIndex(Predicate *predicates, size_t pred_num, Campaign *campaigns,
                  size_t camp_num, Conjunct *unindexed, size_t unindexed_num,
                  std::vector<ConjVec>&& indexed,
                  EntryAttrIndexInterface **entry_indexes,
                  size_t entry_indexes_num);

    CampaignIndex(CampaignIndex &other) = delete;
    CampaignIndex& operator=(CampaignIndex &other) = delete;
    CampaignIndex(CampaignIndex &&other);               //move constructor
    CampaignIndex& operator=(CampaignIndex &&other);    //move assignment operator
    ~CampaignIndex();

public:
    size_t campaignNum() const;
    size_t indexedNum() const;
    size_t unindexedNum() const;

    /*
     * The function that is responsible for reporting the matching campaigns.
     * Input:  AM record
     * Output: a bitset having '1' in positions where the corrresponding
     *         campaigns matched
     * It is the client's responsibility to delete the bitset when it is done
     * with it.
     */
    Bitset matchCampaigns(const AMRecord &record) const;
private:
    EntryAttrIndexInterface **_entry_indexes = nullptr;
    size_t _entry_index_num = 0;
    Conjunct *_unindexed = nullptr;
    size_t _unindexed_num = 0;
    Predicate *_predicates = nullptr;     //global represantation of predicates
    size_t _pred_num = 0;
    Campaign *_campaigns = nullptr;
    size_t _campaign_num = 0;
    std::vector<ConjVec> _indexed;

private:
    /*
     * It is fed with an array containing the Elements of an entry index whose
     * keys overlapped with the corresponding AM attribute value. Each Element
     * is a pointer to a vector of conjuncts.
     */
    void evaluateConjuncts(Bitset&, Element*, ushort array_size,
                           const AMRecord &record) const;

    /*
     * Evaluates all unindexed conjuncts one after the other. If a conjunct
     * evaluates to true, we set the corresponding bit in the bitset.
     */
    void evaluateUnindexed(Bitset &bitset, const AMRecord &record) const;
};
