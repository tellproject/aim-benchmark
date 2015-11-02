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
