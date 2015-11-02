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
#include <vector>

#include "campaign_index_builder.h"

/*
 * SQL queries for retrieving information in order to build the campaign index.
 */
const char* PRED = "SELECT p.id, p.wt_attribute, p.operator, c.value, \
                    c.data_type FROM predicate p, constant c WHERE \
                    p.constant = c.id ORDER BY p.id;";

const char* CAMP = "SELECT * FROM campaign;";

const char* NUM_OF_UNINDEXED = "SELECT COUNT(*) FROM conjunct where id NOT IN (SELECT \
                                distinct cp.conjunct FROM conjunct_predicate cp, \
                                predicate p, wt_attribute wt WHERE cp.predicate = p.id \
                                AND p.wt_attribute = wt.id AND wt.is_pivot = 1);";

const char* UNINDEXED = "SELECT cp.conjunct, cp.predicate, c.campaign FROM \
                         conjunct_predicate cp, conjunct c WHERE conjunct NOT IN \
                         (SELECT cp.conjunct FROM conjunct_predicate cp, \
                         predicate p, wt_attribute wt WHERE cp.predicate = p.id \
                         AND p.wt_attribute = wt.id AND wt.is_pivot= 1) AND \
                         cp.conjunct = c.id ORDER BY conjunct;";

const char* INDEXED = "SELECT cp.predicate, cp.conjunct FROM conjunct_predicate \
                       cp, predicate p, wt_attribute wt WHERE cp.predicate = p.id \
                       AND p.wt_attribute = wt.id AND wt.is_pivot = 1 ORDER BY \
                       cp.predicate;";

const char* NUM_OF_ENTRY_PREDS = "SELECT COUNT(*) from predicate p, wt_attribute \
                                  wt WHERE p.wt_attribute = wt.id AND \
                                  wt.is_pivot = 1;";

const char* CONJ_DETAILS = "SELECT cp.predicate, c.campaign FROM conjunct_predicate cp,\
                            conjunct c WHERE c.id = ? AND c.id = cp.conjunct;";

const char* ENTRY_ATTR = "SELECT id, aggr_data_type FROM wt_attribute WHERE \
                          is_pivot = 1;";

const char* PREDS_PER_ENTRY_ATTR = "SELECT p.id FROM wt_attribute wt, predicate p WHERE \
                                    p.wt_attribute = wt.id AND wt.id = ?;";

const char* PRED_DETAILS = "SELECT p.operator, c.value FROM predicate p, constant c \
                            WHERE p.id = ? AND p.constant = c.id;";

CampaignIndex
CampaignIndexBuilder::build(sqlite3 * conn, const AIMSchema& schema)
{
    // build prepared statements
    const char *pzTail;
    int rc =  sqlite3_prepare_v2(conn, CONJ_DETAILS, strlen(CONJ_DETAILS), &_conj_details_stmt, &pzTail);
    assert(rc == SQLITE_OK);
    rc = sqlite3_prepare_v2(conn, PREDS_PER_ENTRY_ATTR, strlen(PREDS_PER_ENTRY_ATTR), &_preds_per_entry_attr_stmt, &pzTail);
    assert(rc == SQLITE_OK);
    rc = sqlite3_prepare_v2(conn, PRED_DETAILS, strlen(PRED_DETAILS), &_pred_details_stmt, &pzTail);
    assert(rc == SQLITE_OK);

    // build campaign index
    buildPredicates(conn, schema);
    buildUnindexed(conn);
    buildIndexed(conn);
    buildEntryIndexes(conn, schema);
    buildCampaigns(conn);
    CampaignIndex campaignIndex(_predicates, _pred_num, _campaigns, _camp_num,
                                _unindexed, _unindexed_num, std::move(_indexed),
                                _entry_indexes, _entry_index_num);

    // finalize prepared statements
    rc = sqlite3_finalize(_conj_details_stmt);
    assert(rc == SQLITE_OK);
    rc = sqlite3_finalize(_preds_per_entry_attr_stmt);
    assert(rc == SQLITE_OK);
    rc = sqlite3_finalize(_pred_details_stmt);
    assert(rc == SQLITE_OK);

    // return campaign index
    return campaignIndex;
}

inline const char * sqlite3_signed_column_text(sqlite3_stmt* stmt, int iCol)
{
    return reinterpret_cast<const char *>(sqlite3_column_text(stmt, iCol));
}

/*
 * This function sets the offset of the AM attribute this predicate involves,
 * the evaluation function (<,>,=,..) and the constant.
 */
void
CampaignIndexBuilder::buildPredicates(sqlite3 * conn, const AIMSchema& schema)
{

    // prepare statement
    sqlite3_stmt *stmt;
    const char *pzTail;
    int rc = sqlite3_prepare_v2(conn, PRED, strlen(PRED), &stmt, &pzTail);
    assert(rc == SQLITE_OK);
    assert(stmt != NULL);

    // process statement
    std::vector<Predicate> predicates;
    predicates.reserve(128);
    size_t i = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        predicates.emplace_back();
        DataType const_data_type = getDataType(sqlite3_signed_column_text(stmt, 4));
        predicates[i].setOffset(schema.OffsetAt(sqlite3_column_int(stmt, 1) - 1));
        predicates[i].setEvalFun(getEvalFun(sqlite3_signed_column_text(stmt, 2), const_data_type));
        setPredConstant(predicates[i], sqlite3_signed_column_text(stmt, 3), const_data_type);
        ++i;
    }
    _pred_num = predicates.size();
    _predicates = new Predicate[_pred_num];
    std::memcpy(reinterpret_cast<void*>(_predicates), reinterpret_cast<void*>(predicates.data()), sizeof(Predicate)*_pred_num);

    assert(rc == SQLITE_DONE);
    rc = sqlite3_finalize(stmt);
    assert(rc == SQLITE_OK);


// old mysql error handling code
//    std::cerr << "PRED: " << query.error() << std::endl;
//    assert(false);
//    return;
}

/*
 * Reads the unindexed conjuncts and creates an array containing them. Each
 * conjunct contains the id of the campaign it belongs to as well as an
 * array of predicates.
 */
void
CampaignIndexBuilder::buildUnindexed(sqlite3 * conn)
{
    _unindexed_num = numOfUnindexed(conn);          //find the number of
    if (!_unindexed_num)                            //unindexed conjuncts
        return;
    _unindexed = new Conjunct[_unindexed_num];

    // prepare statement
    sqlite3_stmt *stmt;
    const char *pzTail;
    int rc = sqlite3_prepare_v2(conn, UNINDEXED, strlen(UNINDEXED), &stmt, &pzTail);
    assert(rc == SQLITE_OK);
    assert(stmt != NULL);

    // process statement
    size_t cnt = 0;
    u_int16_t old_camp_id = 0;
    uint16_t new_camp_id = 0;
    uint16_t prev;
    std::vector<Predicate*> pred_ptrs;
    /*
     * As long as the predicates belong to the same conjunct, we keep them
     * together. By the time a predicate belonging to a different conjunct
     * appears we set the predicates of the previous conjuncts.
     */
    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        prev = sqlite3_column_int(stmt, 0);
        do {
            uint16_t cur = sqlite3_column_int(stmt, 0); //cur = new conjunct id
            old_camp_id = new_camp_id;
            new_camp_id = (uint16_t)(sqlite3_column_int(stmt, 2) - 1);
            if (cur != prev) {
                _unindexed[cnt].setCampaignId((uint16_t)(old_camp_id));
                _unindexed[cnt].setPredicates(pred_ptrs);
                ++cnt;
                pred_ptrs.clear();
            }
            pred_ptrs.push_back(&(_predicates[sqlite3_column_int(stmt, 1) - 1]));
            prev = cur;
        } while ((rc = sqlite3_step(stmt)) == SQLITE_ROW);
        _unindexed[cnt].setCampaignId(new_camp_id);
        _unindexed[cnt].setPredicates(pred_ptrs);
    }

    assert(rc == SQLITE_DONE);
    rc = sqlite3_finalize(stmt);
    assert(rc == SQLITE_OK);

// old mysql error handling code
//    std::cerr << "UNINDEXED: " << query.error() << std::endl;
//    assert(false);
//    return;
}

/*
 * Populates the _indexed. Also populates the _preds_to_conj_vec. In this map
 * we store pairs: entry attribute id -> vector<Conjunct>*. We will use this
 * structure for creating the interval trees.
 */
void
CampaignIndexBuilder::buildIndexed(sqlite3 * conn)
{
    uint16_t num_of_entry_preds = numOfEntryPreds(conn);
    if (!num_of_entry_preds)              //no need to do the following processing
        return;                           //if non entry predicates exist

    _indexed.reserve(num_of_entry_preds);

    // prepare statement
    sqlite3_stmt *stmt;
    const char *pzTail;
    int rc = sqlite3_prepare_v2(conn, INDEXED, strlen(INDEXED), &stmt, &pzTail);
    assert(rc == SQLITE_OK);
    assert(stmt != NULL);

    // process statement
    uint16_t cur, prev;
    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        prev = sqlite3_column_int(stmt, 0);
        ConjVec entry_conjs;              //the conjuncts an entry predicate belongs to
        do {
            cur = sqlite3_column_int(stmt, 0);  //read new entry predicate id
            if (cur != prev) {
                _indexed.push_back(std::move(entry_conjs));
                ConjVec *conj_vec_ptr = &(_indexed.at(_indexed.size() - 1));
                _pred_to_conj_vec.insert(std::make_pair(prev, conj_vec_ptr));
                entry_conjs.clear();
            }
            Conjunct conj = buildConjunct(conn, sqlite3_column_int(stmt, 1), cur);
            entry_conjs.emplace_back(std::move(conj));
            prev = cur;
        } while ((rc = sqlite3_step(stmt)) == SQLITE_ROW);
        _indexed.push_back(std::move(entry_conjs));
        ConjVec *conj_vec_ptr = &(_indexed.at(_indexed.size() - 1));
        _pred_to_conj_vec.insert(std::make_pair(cur, conj_vec_ptr));
    }

    assert(rc == SQLITE_DONE);
    rc = sqlite3_finalize(stmt);
    assert(rc == SQLITE_OK);

// old mysql error handling code
//    std::cerr << "INDEXED: " << query.error() << std::endl;
//    assert(false);
//    return;
}

/*
 * Creates an array of entry indexes. Initially we use a vector to store the
 * trees, since we do not know their exact number. Afterwards we allocate the
 * proper number of positions in an array.
 */
void
CampaignIndexBuilder::buildEntryIndexes(sqlite3 * conn, const AIMSchema &schema)
{
    if (!_indexed.size())  {                         //we have no entry predicates
        _entry_index_num = 0;
        return;
    }

    // prepare statement
    sqlite3_stmt *stmt;
    const char *pzTail;
    int rc = sqlite3_prepare_v2(conn, ENTRY_ATTR, strlen(ENTRY_ATTR), &stmt, &pzTail);
    assert(rc == SQLITE_OK);
    assert(stmt != NULL);

    // process statement
    std::vector<EntryAttrIndexInterface*> trees;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        uint16_t attr_id = static_cast<uint16_t>(sqlite3_column_int(stmt, 0) - 1);
        /*
         * We need the offset of the AM attribute this index is built upon.
         * This way we can probe the index using the correct AM attribute value.
         */
        uint16_t offset = schema.OffsetAt(attr_id);
        prepareEntryIndex(conn, sqlite3_column_int(stmt, 0), offset, trees, sqlite3_signed_column_text(stmt, 1));
    }
    assert(rc == SQLITE_DONE);

    _entry_index_num = trees.size();
    _entry_indexes = new EntryAttrIndexInterface*[_entry_index_num];
    for (size_t i=0; i<_entry_index_num; ++i) {
        _entry_indexes[i] = trees[i];
    }

    rc = sqlite3_finalize(stmt);
    assert(rc == SQLITE_OK);

// old mysql error handling code
//    std::cerr << "ENTRY_ATTR: " << query.error() << std::endl;
//    assert(false);
//    return;
}

void
CampaignIndexBuilder::prepareEntryIndex(sqlite3 * conn, uint16_t attr_id,
                                        uint16_t offset, TreeVector &trees,
                                        const char * s_attr_type)
{
    //find all predicates containing a given entry attribute

    // bind parameters of statement
    int rc = sqlite3_reset(_preds_per_entry_attr_stmt);
    assert(rc == SQLITE_OK);
    sqlite3_bind_int(_preds_per_entry_attr_stmt, 1, attr_id);
    assert(rc == SQLITE_OK);

    // process statement
    std::vector<uint16_t> entry_preds;
    while ((rc = sqlite3_step(_preds_per_entry_attr_stmt)) == SQLITE_ROW)
    {
        //keep all predicate ids involving this attribute
        entry_preds.push_back(sqlite3_column_int(_preds_per_entry_attr_stmt, 0));
    }
    assert(rc == SQLITE_DONE);

    if (!entry_preds.size())
        return;

    if (strcmp(s_attr_type,"int") == 0)
        trees.push_back(buildEntryIndex<int>(conn, offset, entry_preds));

    else if (strcmp(s_attr_type,"uint") == 0)        //e.g. sum of calls
        trees.push_back(buildEntryIndex<uint>(conn, offset, entry_preds));

    else if (strcmp(s_attr_type,"double") == 0)      //e.g. max cost
        trees.push_back(buildEntryIndex<double>(conn, offset, entry_preds));

    else if (strcmp(s_attr_type,"ulong") == 0)
        trees.push_back(buildEntryIndex<ulong>(conn, offset, entry_preds));

// old mysql error handling code
//    std::cerr << "PREDS_PER_ENTRY_ATTR: " << query.error() << std::endl;
//    assert(false);
//    return;
}

/*
 * Creates the bounds of the key for a given entry predicate and it associates
 * a key's bounds to the conjuncts containing this predicate. To be more
 * specific, each key is asccociated to a vector<Conjunct>*. Aftrerwards it
 * creates a new entry index for this attribute.
 */
template <typename Key>
EntryAttrIndex<Key>*
CampaignIndexBuilder::buildEntryIndex(sqlite3 * conn, uint16_t offset,
                                      std::vector<uint16_t> pred_ids)
{
    typedef Key KeyRange[2];
    typedef KeyRange Bounds;
    typedef std::vector<Conjunct>* Element;

    Element *elements = new Element[pred_ids.size()];
    Bounds *bounds = new KeyRange[pred_ids.size()];

    for (size_t i=0; i<pred_ids.size(); ++i) {
        // bind parameters of statement
        int rc = sqlite3_reset(_pred_details_stmt);
        assert(rc == SQLITE_OK);
        sqlite3_bind_int(_pred_details_stmt, 1, pred_ids[i]);
        assert(rc == SQLITE_OK);

        if ((rc = sqlite3_step(_pred_details_stmt)) == SQLITE_ROW) {
            setBounds<Key>(sqlite3_signed_column_text(_pred_details_stmt, 0), sqlite3_column_double(_pred_details_stmt, 1), bounds[i]);
            elements[i] = _pred_to_conj_vec.find(pred_ids[i])->second;
        }

// old mysql error handling code
//        std::cerr << "PRED_DETAILS: " << query.error() << std::endl;
//        assert(false);
//        return nullptr;
    }
    EntryAttrIndex<Key>* tree = new EntryAttrIndex<Key>(offset, elements, bounds,
                                                        (ushort)pred_ids.size());
    delete [] elements;
    delete [] bounds;
    return tree;
}

/*
 * Since we only store closed closed intervals to the tree, we must adjust the
 * value of the predicates accordingly. This function is used for int and long
 * values. E.g. x >= 5 -> [5, std::numeric_limits<int>::max()]
 */
template <typename Key>
void
CampaignIndexBuilder::setBounds(const char * s_op, Key value, Key bound_ptr[2]) const
{
    if (strcmp(s_op,"lte") == 0) {
        bound_ptr[0] = rtree::min<Key>();
        bound_ptr[1] = value;
    }
    else if (strcmp(s_op,"lt") == 0) {
        bound_ptr[0] = rtree::min<Key>();
        bound_ptr[1] = --value;
    }
    else if (strcmp(s_op,"e") == 0) {
        bound_ptr[0] = value;
        bound_ptr[1] = value;
    }
    else if (strcmp(s_op,"gre") == 0) {
        bound_ptr[0] = value;
        bound_ptr[1] = rtree::max<Key>();
    }
    else if (strcmp(s_op,"gr") == 0) {
        bound_ptr[0] = ++value;
        bound_ptr[1] = rtree::max<Key>();
    }
    else if (strcmp(s_op,"like") == 0) {
        assert(false);
        return;
    }
}

/*
 * Specialization of the above template function for double types.
 */
template<>
void
CampaignIndexBuilder::setBounds<double>(const char * s_op, double value,
                                        double bound_ptr[2]) const
{
    if (strcmp(s_op,"lte") == 0) {
        bound_ptr[0] = rtree::min<double>();
        bound_ptr[1] = value;
    }
    else if (strcmp(s_op,"lt") == 0) {
        bound_ptr[0] = rtree::min<double>();
        bound_ptr[1] = nextafter(value, -999.0); //the closest representatible
    }
    else  if (strcmp(s_op,"e") == 0) {
        bound_ptr[0] = value;
        bound_ptr[1] = value;
    }
    else if (strcmp(s_op,"gre") == 0) {
        bound_ptr[0] = value;
        bound_ptr[1] = rtree::max<double>();
    }
    else if (strcmp(s_op,"gr") == 0) {
        bound_ptr[0] = nextafter(value, 999.0); //the closest representatible
        bound_ptr[1] = rtree::max<double>();
    }
    else if (strcmp(s_op,"like") == 0) {
        assert(false);
        return;
    }
}

void
CampaignIndexBuilder::buildCampaigns(sqlite3 * conn)
{

    // prepare statement
    sqlite3_stmt *stmt;
    const char *pzTail;
    int rc = sqlite3_prepare_v2(conn, CAMP, strlen(CAMP), &stmt, &pzTail);
    assert(rc == SQLITE_OK);
    assert(stmt != NULL);

    // process statement
    std::vector<Campaign> campaigns;
    campaigns.reserve(128);
    size_t i = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        campaigns.emplace_back();
        FiringInterval interval = getInterval(sqlite3_signed_column_text(stmt, 3));
        FiringStartCond start_cond = getStartCondition(sqlite3_signed_column_text(stmt, 4));
        campaigns[i].setId(sqlite3_column_int(stmt, 0));
        campaigns[i].setValidFrom(sqlite3_column_int64(stmt, 1));
        campaigns[i].setValidTo(sqlite3_column_int64(stmt, 2));
        campaigns[i].setFiringInterval(interval);
        campaigns[i].setFiringStartCond(start_cond);
        campaigns[i].setFireCheck(getFireCheckFun(interval, start_cond));
        ++i;
    }
    assert(rc == SQLITE_DONE);

    _camp_num = campaigns.size();
    _campaigns = new Campaign[_camp_num];
    std::memcpy(reinterpret_cast<void*>(_campaigns), reinterpret_cast<void*>(campaigns.data()), sizeof(Campaign)*_camp_num);

    rc = sqlite3_finalize(stmt);
    assert(rc == SQLITE_OK);

// old mysql error handling code
//    std::cerr << "Failed to execute CAMP: " << query.error() << std::endl;
//    assert(false);
//    return;
}

/*
 * Executes a query resulting in the number of unindexed conjuncts.
 */
int32_t
CampaignIndexBuilder::numOfUnindexed(sqlite3 * conn)
{
    // prepare statement
    sqlite3_stmt *stmt;
    const char *pzTail;
    int rc = sqlite3_prepare_v2(conn, NUM_OF_UNINDEXED, strlen(NUM_OF_UNINDEXED), &stmt, &pzTail);
    assert(rc == SQLITE_OK);
    assert(stmt != NULL);

    // process statement
    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        int result = sqlite3_column_int(stmt, 0);

        rc = sqlite3_finalize(stmt);
        assert(rc == SQLITE_OK);
        return result;
    }
    else
    {
        std::cerr << "NUM_OF_UNINDEXED: " << rc << std::endl;
        assert(false);
        return -1;
    }
}

/*
 * Executes a query resulting in the number of entry predicates.
 */
int32_t
CampaignIndexBuilder::numOfEntryPreds(sqlite3 * conn)
{
    // prepare statement
    sqlite3_stmt *stmt;
    const char *pzTail;
    int rc = sqlite3_prepare_v2(conn, NUM_OF_ENTRY_PREDS, strlen(NUM_OF_ENTRY_PREDS), &stmt, &pzTail);
    assert(rc == SQLITE_OK);
    assert(stmt != NULL);

    // process statement
    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        int result = sqlite3_column_int(stmt, 0);

        rc = sqlite3_finalize(stmt);
        assert(rc == SQLITE_OK);
        return result;
    }
    else
    {
        std::cerr << "NUM_OF_ENTRY_PREDS: " << rc << std::endl;
        assert(false);
        return -1;
    }
}

/*
 * For a given conjunct id retrieves the predicates existing in this conjunct.
 * This conjunct will later be placed at a leaf of an entry index for an entry
 * predicate. For this reason is redundant to include the entry predicate in the
 * conjunct.
 */
Conjunct
CampaignIndexBuilder::buildConjunct(sqlite3 * conn, uint16_t conj_id,
                                    uint16_t entry_pred_id)
{
    // bind parameters of statement
    int rc = sqlite3_reset(_conj_details_stmt);
    assert(rc == SQLITE_OK);
    sqlite3_bind_int(_conj_details_stmt, 1, conj_id);
    assert(rc == SQLITE_OK);

    // process statement
    std::vector<Predicate*> pred_ptr_vec;
    uint16_t pred_id, camp_id = 0;
    while ((rc = sqlite3_step(_conj_details_stmt)) == SQLITE_ROW) {
        pred_id = sqlite3_column_int(_conj_details_stmt, 0);
        camp_id = sqlite3_column_int(_conj_details_stmt, 1);
        if (pred_id == entry_pred_id)
            continue;
        pred_ptr_vec.push_back(&(_predicates[pred_id - 1]));
    }
    assert(rc == SQLITE_DONE);

    //pass campaign id, vector<Pred*>
    Conjunct conjunct((uint16_t)(camp_id - 1), pred_ptr_vec);
    return conjunct;

// old mysql error handling code
//    std::cerr << "CONJ_DETAILS: " << query.error() << std::endl;
//    assert(false);
//    return decltype(buildConjunct(conn, conj_id, entry_pred_id))();
}

void
CampaignIndexBuilder::setPredConstant(Predicate &pred, const char *
                                      s_constant, const DataType type)
{
    switch (type) {
    case DataType::INT:
    {
        int i_constant = std::stoi(s_constant);
        pred.setConstant(i_constant);
        return;
    }
    case DataType::UINT:
    {
        uint ui_constant = std::stoul(s_constant);
        pred.setConstant(ui_constant);
        return;
    }
    case DataType::ULONG:
    {
        ulong ul_constant = std::stoull(s_constant);
        pred.setConstant(ul_constant);
        return;
    }
    case DataType::DOUBLE:
    {
        double d_constant = std::stold(s_constant);
        pred.setConstant(d_constant);
        return;
    }
    }
}

Predicate::EvalFPtr
CampaignIndexBuilder::getEvalFun(const char * s_operator,
                                 const DataType data_type) const
{

    if (strcmp(s_operator,"lte") == 0)
        return setLteFun(data_type);
    if (strcmp(s_operator,"lt") == 0)
        return setLtFun(data_type);
    if (strcmp(s_operator,"e") == 0)
        return setEqFun(data_type);
    if (strcmp(s_operator,"gre") == 0)
        return setGreFun(data_type);
    if (strcmp(s_operator,"gr") == 0)
        return setGrFun(data_type);
    if (strcmp(s_operator,"like") == 0)
        return setLikeFun(data_type);
    assert(false);
    return setLteFun(data_type);
}

DataType
CampaignIndexBuilder::getDataType(const char * s_data_type) const
{
    if (strcmp(s_data_type,"int") == 0)
        return DataType::INT;
    if (strcmp(s_data_type,"uint") == 0)
        return DataType::UINT;
    if (strcmp(s_data_type,"double") == 0)
        return DataType::DOUBLE;
    if (strcmp(s_data_type,"ulong") == 0)
        return DataType::ULONG;
    assert(false);
    return DataType::INT;
}

Predicate::EvalFPtr
CampaignIndexBuilder::setLteFun(const DataType data_type) const
{
    switch (data_type) {
    case DataType::INT:
        return lte<int>;
    case DataType::UINT:
        return lte<uint>;
    case DataType::ULONG:
        return lte<ulong>;
    case DataType::DOUBLE:
        return lte<double>;
    default:
        assert(false);
        return lte<int>;
    }
}

Predicate::EvalFPtr
CampaignIndexBuilder::setLtFun(const DataType data_type) const
{
    switch (data_type) {
    case DataType::INT:
        return lt<int>;
    case DataType::UINT:
        return lt<uint>;
    case DataType::ULONG:
        return lt<ulong>;
    case DataType::DOUBLE:
        return lt<double>;
    default:
        assert(false);
        return lt<int>;
    }
}

Predicate::EvalFPtr
CampaignIndexBuilder::setEqFun(const DataType data_type) const
{
    switch (data_type) {
    case DataType::INT:
        return eq<int>;
    case DataType::UINT:
        return eq<uint>;
    case DataType::ULONG:
        return eq<ulong>;
    case DataType::DOUBLE:
        return eq<double>;
    default:
        assert(false);
        return eq<int>;
    }
}

Predicate::EvalFPtr
CampaignIndexBuilder::setGreFun(const DataType data_type) const
{
    switch (data_type) {
    case DataType::INT:
        return gre<int>;
    case DataType::UINT:
        return gre<uint>;
    case DataType::ULONG:
        return gre<ulong>;
    case DataType::DOUBLE:
        return gre<double>;
    default:
        assert(false);
        return gre<int>;
    }
}

Predicate::EvalFPtr
CampaignIndexBuilder::setGrFun(const DataType data_type) const
{
    switch (data_type) {
    case DataType::INT:
        return gr<int>;
    case DataType::UINT:
        return gr<uint>;
    case DataType::ULONG:
        return gr<ulong>;
    case DataType::DOUBLE:
        return gr<double>;
    default:
        assert(false);
        return gr<int>;
    }
}

Predicate::EvalFPtr
CampaignIndexBuilder::setLikeFun(const DataType data_type) const
{
    //TODO: introduce LIKE predicates in the system.
    return nullptr;
}

FiringInterval
CampaignIndexBuilder::getInterval(const char * s_interval) const
{
    if (strcmp(s_interval,"0") == 0)
        return FiringInterval::ALWAYS;
    if (strcmp(s_interval,"1d") == 0)
        return FiringInterval::ONEDAY;
    if (strcmp(s_interval,"2d") == 0)
        return FiringInterval::TWODAYS;
    if (strcmp(s_interval,"w") == 0)
        return FiringInterval::ONEWEEK;
    assert(false);
    return FiringInterval::ALWAYS;
}

FiringStartCond
CampaignIndexBuilder::getStartCondition(const char * s_start_cond) const
{
    return (strcmp(s_start_cond,"fixed") == 0) ? FiringStartCond::FIXED : FiringStartCond::SLIDING;
}

Campaign::FireFPtr
CampaignIndexBuilder::getFireCheckFun(FiringInterval interval,
                                      FiringStartCond start_cond) const
{
    switch (interval) {
    case FiringInterval::ALWAYS:
        return &always;
    case FiringInterval::ONEDAY:
        return (start_cond == FiringStartCond::FIXED) ? &oneDayFixed: &oneDaySliding;
    case FiringInterval::TWODAYS:
        return (start_cond == FiringStartCond::FIXED) ? &twoDayFixed: &twoDaySliding;
    case FiringInterval::ONEWEEK:
        return (start_cond == FiringStartCond::FIXED) ? &oneWeekFixed: &oneWeekSliding;
    default:
        assert(false);
        return &always;
    }
}
