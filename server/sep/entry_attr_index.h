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

#include <vector>
#include <unordered_map>

#include "sep/am_record.h"
#include "sep/conjunct.h"
#include "sep/OneDimRTree.h"

using namespace rtree;

/*
 * Base class for the entryAttrIndex containing virtual functions being
 * implemented by the child class. We do this in order to be able to have an
 * array of a common type. Now we can have a EntryAttrIndexInterface **array
 * and then it is possible to cast each position to a different data type
 * (int, uint, long, etc).
 */
class EntryAttrIndexInterface {
public:
    virtual ~EntryAttrIndexInterface() {}
public:
    typedef std::vector<Conjunct>* Element;
    virtual ushort candConjuncts(const AMRecord &record,
                                 Element **found) const = 0;
    virtual ushort size() const = 0;
};

/*
 * Wraps the interval tree (for inequality entry predicates). It stores the
 * offset within the AM record of the attribute the index is built on.
 * Key:     [lower bound, upper bound] range created based on an entry predicate.
 *          E.g. x >= 5 --> range: [5, std::numeric_limits<int>::max()]
 * Element: pointer to vector of conjuncts this entry predicate belongs to.
 *
 * Future step: add hash map for equality predicates.
 *
 * Sample Usage: EntryAttrIndexInterface tree = new EntryAttrIndex<int>(args);
 *               AMRecord record(args);
 *               Element *result;
 *               int numOfFound = tree->candidateConjuncts(rec, &result);
 *               delete [] result;
 */

template <typename Key>
class EntryAttrIndex : public EntryAttrIndexInterface
{
public:
    /*
     * R-tree template parametetes:
     * Intermediate node fan-out = 4
     * Leaf fan-out = 8
     * Maximum number of elements = 1024
     *
     * By defining them at compile time compiler can do optimizations.
     */
    typedef OneDimRTree<Element, Key, 4, 8, 1024> Tree;
    typedef typename Tree::KeyRange Bounds;
    EntryAttrIndex(uint16_t offset, Element*, Bounds*, ushort num_of_elem);

public:
    ushort size() const override;
    ushort candConjuncts(const AMRecord&, Element**) const override;

private:
    uint16_t _offset;
    Tree tree;
};

/*
 * Constructor: assigns the offset of the value of the AM attribute value
 * within the record and inserts the keys and elements into the tree.
 */
template <typename Key>
EntryAttrIndex<Key>::EntryAttrIndex(uint16_t offset, Element *elems,
                                    Bounds *bounds, ushort num_of_elem)
{
    _offset = offset;
    tree.reset(elems, bounds, num_of_elem);
}

template <typename Key>
inline ushort
EntryAttrIndex<Key>::size() const
{
    return tree.getNoOfElements();
}

/*
 * Given a subscriber profile (AM attributes values) it returns all the
 * elements whose keys overlap with the value of the entry attribute this index
 * is dedicated to. It is the client's responsibility to delete the found array
 * when it is done with it.
 */
template <typename Key>
inline ushort
EntryAttrIndex<Key>::candConjuncts(const AMRecord &record, Element **found) const
{
    *found = new Element[Tree::size];
    Key key;
    memcpy(&key, &record.data()[_offset], sizeof(Key));
    return tree.find(key, *found);
}
