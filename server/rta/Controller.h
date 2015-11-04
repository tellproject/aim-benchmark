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
//#pragma once

//#include <array>

//#include "rta/communication/TCPRTACommunication.h"
//#include "rta/dimension_schema.h"
//#include "util/sparsehash/dense_hash_map"

//class Controller
//{
//public:
//    typedef ColumnMap<DefaultColumnMapArgs<uint64_t, std::equal_to<uint64_t>,
//                      1+(SUBSCR_NUM/SERVER_NUM/RTA_THREAD_NUM), RECORDS_PER_BUCKET> > StorageType;
//    typedef google::dense_hash_map<uint64_t, std::vector<char> > DeltaType;

//    enum class ReadResult
//    {
//        Exists,
//        DoesntExist
//    };

//    /*
//     * This struct carries information about the start and end point
//     * of a record within the ColumnMap. The ColumnMap stores the complete
//     * profile of a user, namely the AM part and the Dimension part. Using
//     * this struct we know when each part starts and ends.
//     */
//    struct ColBounds
//    {
//        uint16_t start;
//        uint16_t end;
//    };

//public:
//    Controller(const AIMSchema& aim_schema,
//               const DimensionSchema& dim_schema,
//               std::unique_ptr<AbstractRTACommunication> rtaModule,
//               bool start_workers);
//    ~Controller();

//public: //Sep related functions
//    /**
//     * @brief read reads a record of the wide table.
//     * The value returned is the newest version of this value that exists in the
//     * system.
//     *
//     * @param key
//     * @param out a reference to a char vector into which the value is written
//     * @return ReadResult::Exists if read successful, ReadResult::DoesntExist otherwise
//     */
//    ReadResult read(uint64_t key, std::vector<char>* out);

//    /**
//     * @brief readFullRecord reads a full record of the wide table joined with the
//     * dimension tables.
//     * The value returned is the newest version of this value that exists in the
//     * system.
//     *
//     * @param key
//     * @param out a reference to a char vector into which the value is written
//     * @return ReadResult::Exists if read successful, ReadResult::DoesntExist otherwise
//     */
//    ReadResult readFullRecord(uint64_t key, std::vector<char>* out);

//    /**
//     * @brief write stores a record of the wide table (without dimension data)
//     * this method eventually overrides the previous value.
//     * @param key
//     * @param buf
//     */
//    void write(uint64_t key, std::vector<char> buf);

//    /**
//     * @brief populate stores a full record of the wide table plus dimension data.
//     * This method should only be used during startup as it writes directly into the
//     * main storage.
//     * @param key
//     * @param value
//     */
//    void populate(uint64_t key, const char* value);

//public:    //USE ONLY FOR TESTING
//    uint __applyUpdates(uint thread_id) {return _update(thread_id);}
//    void __enqueueQuery(AbstractQueryServerObject* query) {_server_module->__enqueueQuery(query);}
//    void __getQueuedQueries(uint thread_id) {_getQueuedQueries(thread_id);}
//    void __scan(uint thread_id) {_scan(thread_id);}
//    void __initStorage(uint thread_id){ return _initStorage(thread_id);}
//    AbstractRTACommunication* __rtaCommunication(){return _server_module.get();}

//private:    //Update related functions
//    /**
//     * @brief update merges the values from delta into storage. This function is
//     * called once a thread has fully scanned through storage.
//     * @param thread_id  this thread's identifier
//     */
//    uint _update(uint threadId);

//    /**
//     * @brief switchDeltas waits until there are no SEP threads writing to the
//     * current delta. It then moves the current delta to the old delta and
//     * creates a new current delta.
//     *
//     * @param thread_id  the thread's identifier
//     * @return true if a new delta was allocated, false if there was no need for this because delta was empty.
//     */
//    bool _switchDeltas(uint threadId);

//private:    //Scan related functions
//    /**
//     * @brief scan iterates through the storage and passes each tuple to
//     *        all currently available query objects. Once the scan is finished,
//     *        .end() is called on each query object to let the rta-client know
//     *        that there won't be any more calls to processTuple() for this
//     *        particular query object.
//     *
//     *        Once a full iteration of storage is done, scan calls update
//     *        which will take care of merging new values into storage
//     *
//     * @param thread_id  this thread's identifier
//     * @return double   number of nanoseconds used for a full iteration of storage
//     */
//    double _scan(uint thread_id);

//    /**
//     * @brief scanStarted prints message if logging is enabled
//     * @param thread_id  this thread's identifier
//     *                  parameter only used for debugging purposes
//     */
//    void _scanStarted(uint threadId);

//    /**
//     * @brief Controller::scanFinished decrements the number of running scans
//     * @param thread_id  this thread's identifier,
//     *                  parameter only used for debugging purposes
//     */
//    void _scanFinished(uint threadId);

//    /**
//     * @brief getQueuedQueries fetches all available active queries from
//     * the server module and stores them in the Controller. This function is called
//     * periodically by thread with thread_id 0.
//     *
//     * @return void
//     */
//    void _getQueuedQueries(uint thread_id);

//    // getter methods for scan
//    int _getScanGeneration();
//    size_t _getNumQueries();
//    std::vector<AbstractQueryServerObject*>& _getActiveQueries();


//private:    //General purpose functions
//    void _initStorage(uint thread_id);

//    /**
//     * @brief A worker thread maintains its own Storage and Delta objects and
//     *        is responsible for scanning and updating the storage.
//     *
//     *        worker is started by the constructor of Controller as a thread.
//     *        Worker continuously gets new queries and starts the scan function
//     *        until SEP has requested the Controller to shutdown the system.
//     */
//    void _worker(uint thread_id);

//    void _acquireSepLock(uint thread_id);
//    void _releaseSepLock(uint thread_id);

//private:
//    struct ThreadStorage
//    {
//         ThreadStorage(const vector<uint16_t>& col_sizes):
//             delta_count(0),
//             delta(new DeltaType(DELTA_ENTRY_ALLOC_HINT)),
//             old_delta(new DeltaType(DELTA_ENTRY_ALLOC_HINT)),
//             storage(col_sizes),
//             rta_is_ready_to_update_storage(false),
//             sep_is_reading_and_writing(false)
//         {
//             delta->set_empty_key(-1);
//             delta->set_deleted_key(-2);
//             old_delta->set_empty_key(-1);
//             old_delta->set_deleted_key(-2);
//         }

//         std::atomic<size_t> delta_count;
//         std::unique_ptr<DeltaType> delta;
//         std::unique_ptr<DeltaType> old_delta;
//         StorageType storage;
//         //RTA's scan thread is ready to merge the delta into storage
//         std::atomic<bool> rta_is_ready_to_update_storage;
//         //SEP is currently accessing the current and old delta and storage
//         mutable std::atomic<bool> sep_is_reading_and_writing;
//     };

//private:
//    static constexpr size_t _rta_thread_num = RTA_THREAD_NUM;
//    std::vector<std::thread> _workers;
//    std::array<std::mutex, RTA_THREAD_NUM> _populate_mutexes;

//    std::vector<std::unique_ptr<ThreadStorage> > _thread_storages;
//    std::atomic<int> _init_counter;

//    std::vector<uint16_t> _col_sizes;
//    ColBounds _aim_bounds;
//    ColBounds _dim_bounds;

//    std::unique_ptr<AbstractRTACommunication> _server_module;
//    std::atomic<int> _running_scans;
//    std::atomic<int> _scan_generation;
//    std::vector<AbstractQueryServerObject*> _active_queries;

//    std::atomic<bool> _shutdown;
//};
