#pragma once

#include <inttypes.h>
#include <set>
#include <unordered_map>
#include <string>

#include "util/sqlite/sqlite3.h"

#include "sep/campaign_index.h"

/*
 * Class responsible for building the campaign index based on the information
 * written in the meta-database (Sqlite). It creates an array of all predicates
 * (global representation of predicates) and an array of the campaigns. It also
 * creates an array of the unindexed conjuncts and finally it builds the entry
 * indexes. These indexes are built on entry attributes and they index entry
 * predicates. At leaf level we store pointers to vectors of Conjuncts (rather
 * than single conjuncts), since an entry predicate may participate in multiple
 * conjuncts. The building process is executed offline (before the bechmark
 * starts). We have taken the condition logic out of the campaigs. Conditions
 * are evaluated by examining the unindexed conjuncs as well as the conjuncts
 * being returned by the entry indexes. Campaigns store only their other
 * properties (validity period and firing policy).
 *
 * Sample Usage: sqlite3 *conn;
 *               sqlite3_open_v2("meta_db.db", &conn, SQLITE_OPEN_READONLY, NULL);
 *               Schema s;
 *               CampaignIndexBuilder c_i_b = new CampaignIndexBuilder(conn, s);
 *               CampaignIndex c_i = c_i_b.build();
 */
class CampaignIndexBuilder
{
public:
    typedef std::vector<Conjunct> ConjVec;
    typedef typename std::unordered_map<uint16_t, ConjVec*> Map;
    typedef typename std::vector<EntryAttrIndexInterface*> TreeVector;

public:
    CampaignIndex build(sqlite3 *, const AIMSchema&);

private:
    Predicate *_predicates = nullptr;
    size_t _pred_num = 0;
    Campaign *_campaigns = nullptr;
    size_t _camp_num = 0;
    Conjunct *_unindexed = nullptr;
    size_t _unindexed_num = 0;
    std::vector<ConjVec> _indexed;
    //maps entry predicates to vector<Conjunct>*
    Map _pred_to_conj_vec;
    EntryAttrIndexInterface **_entry_indexes = nullptr;
    size_t _entry_index_num = 0;

    //sqlite3 parametrized prepared statements
    sqlite3_stmt *_conj_details_stmt;
    sqlite3_stmt *_preds_per_entry_attr_stmt;
    sqlite3_stmt *_pred_details_stmt;

private:
    /*
     * Builds the array of the existing predicates. We keep predicates globally.
    */
    void buildPredicates(sqlite3 *, const AIMSchema&);

    /*
     * Builds the array of the unindexed conjuncts, namely conjuncts containing
     * non entry predicate.
     */
    void buildUnindexed(sqlite3 *);

    /*
     * Prepares Entry Indexes building. It populates _preds_to_conj_vec data
     * structure that maps entry predicates to conjuncts containing them.
     */
    void buildIndexed(sqlite3 *);

    /*
     * For each entry attribute it builds an R-tree. For the actual building
     * process it invokes the prepareEntryIndex.
     */
    void buildEntryIndexes(sqlite3 *, const AIMSchema&);

    /*
     * For an entry attribute finds the corresponding entry predicates and based
     * on the data type of the attribute it creates the proper tree.
     */
    void prepareEntryIndex(sqlite3 *, uint16_t attr_id, uint16_t offset,
                           TreeVector&, const char * s_attr_type);

    /*
     * Uses the mapping between predicates and conjuncts (_preds_to_conj_vec)
     * and the MySQL database to build a single entryIndex based on the actual
     * data type (int, uint, double..) of the entry attribute. It passes the
     * offset of the AM attribute within the record to the constructor of the
     * tree. Also it uses the entry predicates ids to prepare predicate
     * indexing.
     */
    template <typename Key>
    EntryAttrIndex<Key>* buildEntryIndex(sqlite3 *, uint16_t offset,
                                         std::vector<uint16_t> pred_ids);

    /*
     * A key in the R-tree consists of a range [low, high]. This function sets
     * the bounds of each key will be put into the tree. It parses the operator
     * type retrieved from MySQL to adjust bounds properly.
     */
    template <typename Key>
    void setBounds(const char * s_oper, Key value, Key bound_ptr[2]) const;

    /*
     * Builds an array of campaigns. In our implementation campaigns contains
     * all the properties, but the condition one. The condition properties are
     * implemented using global predicates, conjuncts and entry indexes.
     */
    void buildCampaigns(sqlite3 *);

    /*
     * Returns the number of unindexed conjuncts after executing the
     * respective query in the database.
     */
    int32_t numOfUnindexed(sqlite3 *);

    /*
     * Returns the number of entry predicates. It is used for allocating space
     * for the _indexed vector.
     */
    int32_t numOfEntryPreds(sqlite3 *);

    /*
     * Creates a conjunct by retrieving information from the db. It is used only
     * for conjuncts containing entry predicates. We use the conjunct id and
     * also the id of the entry predicate. The latter is necessary because the
     * conjunct will not contain this predicate. Conjuncts created by this
     * function will be used as values in entry indexes, so it is a repetition
     * to keep the entry predicate.
     */
    Conjunct buildConjunct(sqlite3 *, uint16_t conj_id,
                           uint16_t entry_pred_id);

    /*
     * Parses the retrieved value of a predicate constant based on its data
     * type.
     */
    void setPredConstant(Predicate&, const char *, const DataType);

    /*
     * Determines the evaluation function based on the retrieved operator
     * value.
     */
    Predicate::EvalFPtr getEvalFun(const char *, const DataType) const;

    /*
     * Parses retrieved data_type and tranforms it into a valid data type in
     * program's scope.
     */
    DataType getDataType(const char *) const;

    /*
     * These functions set evaluation function pointer to the correct template
     * parameter based on the data type of the constant.
     */
    Predicate::EvalFPtr setLteFun(const DataType) const;
    Predicate::EvalFPtr setLtFun(const DataType) const;
    Predicate::EvalFPtr setEqFun(const DataType) const;
    Predicate::EvalFPtr setGreFun(const DataType) const;
    Predicate::EvalFPtr setGrFun(const DataType) const;
    Predicate::EvalFPtr setLikeFun(const DataType) const;

    /*
     * Functions parsing the information read from the database.
     */
    FiringInterval getInterval(const char *) const;
    FiringStartCond getStartCondition(const char *) const;
    Campaign::FireFPtr getFireCheckFun(FiringInterval, FiringStartCond) const;
};
