#include "Controller.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>

#include "util/system-constants.h"
#include "logger.h"
#include "util/folly/ScopeGuard.h"

Controller::Controller(const AIMSchema& aim_schema,
                       const DimensionSchema& dim_schema,
                       std::unique_ptr<AbstractRTACommunication> rtaModule,
                       bool start_workers):
    _thread_storages(_rta_thread_num),
    _server_module(std::move(rtaModule))
{
    _shutdown.store(false);
    _running_scans.store(0);
    _scan_generation.store(0);
    _init_counter.store(0);

    // Build a vector containing all the column sizes (aim + dim)
    auto aim_col_sizes = aim_schema.sizes();
    auto dim_col_sizes = dim_schema.sizes();
    _col_sizes.reserve(aim_col_sizes.size() + dim_col_sizes.size());
    _col_sizes.insert(_col_sizes.begin(), aim_col_sizes.begin(), aim_col_sizes.end());
    _col_sizes.insert(_col_sizes.end(), dim_col_sizes.begin(), dim_col_sizes.end());

    _aim_bounds.start = 0;
    _aim_bounds.end = aim_col_sizes.size();
    _dim_bounds.start = aim_col_sizes.size();
    _dim_bounds.end = aim_col_sizes.size() + dim_col_sizes.size();

    if (start_workers) {
        for (size_t i=0; i<_rta_thread_num; ++i) {
            LOG_INFO("Start worker\n");
            _workers.emplace_back(&Controller::_worker, this, i);
        }
        while ((uint)_init_counter < _rta_thread_num) {}
    }
}

Controller::~Controller()
{
    // tell worker threads that they should stop scanning/updating
    _shutdown.store(true);

    // wait for workers to finish
    for (std::thread& worker: _workers) {
        worker.join();
    }
}

auto
Controller::read(uint64_t key, std::vector<char>* out) -> ReadResult
{
    assert(_init_counter == _rta_thread_num);
    auto thread_id = key % _rta_thread_num;

    _acquireSepLock(thread_id);
    auto guard = folly::makeGuard([thread_id, this]() {
        _releaseSepLock(thread_id);
    });

    ThreadStorage& thread_storage = *_thread_storages[thread_id];
    ReadResult ret;
    DeltaType::const_iterator rec = thread_storage.delta->find(key);

    if(rec != thread_storage.delta->end()) { //check current delta
        *out = rec->second;
        ret = ReadResult::Exists;
    }
    else {                                  //check old delta
        DeltaType::const_iterator rec = thread_storage.old_delta->find(key);
        if(rec != thread_storage.old_delta->end()) {
            *out = rec->second;
            ret = ReadResult::Exists;
        }
        else {
            // try to lookup in storage
            auto err = thread_storage.storage.read(key, out, _aim_bounds.start,
                                                   _aim_bounds.end);
            if (err) {
                ret = ReadResult::DoesntExist;
            }
            else {
                ret = ReadResult::Exists;
            }
        }
    }
    return ret;
}

Controller::ReadResult Controller::readFullRecord(uint64_t key, std::vector<char> *out)
{
    auto ret = read(key,out);
    std::vector<char> dim_data;
    auto thread_id = key % _rta_thread_num;

    _acquireSepLock(thread_id);
    auto guard = folly::makeGuard([thread_id, this]() {
        _releaseSepLock(thread_id);
    });

    ThreadStorage& thread_storage = *_thread_storages[thread_id];
    auto err = thread_storage.storage.read(key, &dim_data, _dim_bounds.start,
                                           _dim_bounds.end);
    if (err) {
        ret = ReadResult::DoesntExist;
    }
    else {
        ret = ReadResult::Exists;
    }
    out->insert(out->end(), dim_data.begin(), dim_data.end());
    return ret;
}

void
Controller::write(uint64_t key, std::vector<char> buf)
{
    assert(_init_counter == _rta_thread_num);
    auto thread_id = key % _rta_thread_num;

    _acquireSepLock(thread_id);
    auto guard = folly::makeGuard([thread_id, this]() {
        _releaseSepLock(thread_id);
    });

    ThreadStorage& thread_storage = *_thread_storages[thread_id];
    DeltaType::iterator iter = thread_storage.delta->find(key);
    if(iter != thread_storage.delta->end()) {
        iter->second = std::move(buf);
    }
    else {
        thread_storage.delta->insert(make_pair(key, std::move(buf)));
        thread_storage.delta_count++;
    }
}

void
Controller::populate(uint64_t key, const char *value)
{
    assert(_init_counter == _rta_thread_num);
    auto thread_id = key % _rta_thread_num;
    std::lock_guard<std::mutex> lock(_populate_mutexes[thread_id]);
    _thread_storages[thread_id]->storage.write(key, value, _aim_bounds.start,
                                               _dim_bounds.end);
}

/*
 * A spin-lock in switchDeltas(thread_id) makes sure that there are no
 * SEP threads writing to the current delta while the deltas are being
 * switched.
 */
uint
Controller::_update(uint thread_id)
{
    assert(_init_counter == _rta_thread_num);
    if (!_switchDeltas(thread_id)) {
        return 0;
    }

    ThreadStorage& thread_storage = *_thread_storages[thread_id];
    auto dit = thread_storage.old_delta->begin();
    for (; dit!=thread_storage.old_delta->end(); ++dit) {
        thread_storage.storage.write(dit->first, dit->second.data(),
                                     _aim_bounds.start, _aim_bounds.end);
    }
    return thread_storage.old_delta->size();
}


bool
Controller::_switchDeltas(uint thread_id)
{
    assert(_init_counter == _rta_thread_num);
    ThreadStorage& thread_storage = *_thread_storages[thread_id];
    if (thread_storage.delta_count == 0) {
        return false;
    }
    // first set the intent flag
    thread_storage.rta_is_ready_to_update_storage.store(true);

    // now wait for the SEP's flag to become false
    while (thread_storage.sep_is_reading_and_writing.load()) {} //spin

    if (thread_storage.delta->empty()) {
        thread_storage.rta_is_ready_to_update_storage.store(false);
        return false;
    }
    // start switching the deltas
    thread_storage.old_delta = std::move(thread_storage.delta);
    thread_storage.delta_count = 0;

    thread_storage.delta.reset(new DeltaType(DELTA_ENTRY_ALLOC_HINT));
    thread_storage.delta->set_deleted_key(-1);
    thread_storage.delta->set_empty_key(-2);

    // set intent flag to false
    thread_storage.rta_is_ready_to_update_storage.store(false);
    return true;
}

template<typename Queries>
void
scanBucket(uint thread_id, BucketReference bucket_ref, Queries& queries)
{
    for (auto &query: queries) {
        query->processBucket(thread_id, bucket_ref);
    }
}

template<typename Queries>
void
scanLastBucket(uint thread_id, BucketReference bucket_ref, Queries& queries)
{
    for (auto &query: queries) {
        query->processLastBucket(thread_id, bucket_ref);
        query->end();
    }
}

double
Controller::_scan(uint thread_id)
{
    assert(_init_counter == _rta_thread_num);
    assert(!_active_queries.empty());

    _scanStarted(thread_id);
    auto scan_start = std::chrono::high_resolution_clock::now();
    auto& queries = _getActiveQueries();

    auto& thread_storage = *_thread_storages[thread_id];
    auto bucket_num = thread_storage.storage.bucketsInUse();
    for (uint i=0; i<bucket_num-1; ++i) {
        scanBucket(thread_id, thread_storage.storage.getBucket(i), queries);
    }
    scanLastBucket(thread_id, thread_storage.storage.getBucket(bucket_num - 1), queries);
    _scanFinished(thread_id);
    return to_nano_seconds(std::chrono::high_resolution_clock::now() - scan_start);
}

void
Controller::_scanStarted(uint thread_id)
{
    LOG_INFO("T" << thread_id << " started a scan. There are now " <<
             (_running_scans.load()) << " scans running");
}

void
Controller::_scanFinished(uint thread_id)
{
    int scans = (_running_scans.fetch_sub(1))-1;
    if (scans == 0) {
        _scan_generation.fetch_add(1);
    }
    LOG_INFO("T" << thread_id << " stopped a scan. There are now "
             << scans << " scans running.\n");
}

void
Controller::_getQueuedQueries(uint thread_id)
{
    if (thread_id != 0) { //only thread 0 is allowed to get queries
        return;
    }
    //determine whether its time to fetch new queries from the server module
    //this is the case when all threads have answered all available queries
    assert(_running_scans == 0);

    _active_queries = _server_module->getQueuedQueries();
    if (_active_queries.empty())
        return;

    _running_scans = _rta_thread_num;
}

int
Controller::_getScanGeneration()
{
    return _scan_generation;
}

size_t
Controller::_getNumQueries()
{
    return _active_queries.size();
}

std::vector<AbstractQueryServerObject*>&
Controller::_getActiveQueries()
{
    return _active_queries;
}

void pin_to_core(size_t core)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

void
Controller::_initStorage(uint thread_id)
{
    pin_to_core(thread_id);
    _thread_storages[thread_id].reset(new ThreadStorage(_col_sizes));
    ++_init_counter;
}

void
Controller::_worker(uint thread_id)
{
    _initStorage(thread_id);
    while (_init_counter != _rta_thread_num) {};
    LOG_INFO("Worker " << thread_id << " started");
    uint64_t scan_count = 0;

#ifdef INSTRUMENTATION
    double scan_sum = 0.;
    double wait_sum = 0.;
    double update_sum = 0.;
    uint64_t update_count = 0;
    uint64_t row_update_count = 0;
    std::chrono::time_point<std::chrono::system_clock> scan_start, thread_start;
    bool first_scan = true;
#endif

    while(!_shutdown) {
#ifdef INSTRUMENTATION
        auto update_start = std::chrono::high_resolution_clock::now();
#endif
        auto updated = (scan_count % MERGE_INTERVAL == 0) ? _update(thread_id) : 0;     //only update if we reached a merage interval
#ifdef INSTRUMENTATION
        if (updated) {
            row_update_count += updated;
            update_sum += to_nano_seconds(std::chrono::high_resolution_clock::now() - update_start);
            update_count++;
        }
        scan_start = std::chrono::high_resolution_clock::now();
#endif
        _getQueuedQueries(thread_id);
        if (_running_scans == 0) {  // this means queued queries is 0
            continue;
        }

#ifdef INSTRUMENTATION
        if (first_scan) {
            first_scan = false;
            thread_start = std::chrono::high_resolution_clock::now();
        }
#endif

        auto old_generation = _getScanGeneration();
        double scan_runtime = _scan(thread_id);

#ifdef INSTRUMENTATION
        auto wait_start = std::chrono::high_resolution_clock::now();
#endif

        // wait for all scans to finish
        while(_getScanGeneration() == old_generation && !_shutdown) {}

#ifdef INSTRUMENTATION
        auto wait_end = std::chrono::high_resolution_clock::now();
        auto wait_duration = to_nano_seconds(wait_end - wait_start);
        wait_sum += wait_duration;
        scan_sum += (to_nano_seconds(wait_end - scan_start)/1000000);
#endif

        scan_count++;

#ifdef INSTRUMENTATION
        if(thread_id == 0 && (scan_count % 1024) == 0) {
            std::stringstream ss;
            ss << "\nthread " << thread_id << "\ncurrent scan time: " << scan_runtime/1000000 << " ms\n"
               << "avg total scan time (incl. wait): " << scan_sum/ scan_count << " ms\n"
               << "avg update time: " << update_sum/update_count/1000000 << " ms\n"
               << "avg wait time after scan: " << wait_sum/scan_count/1000000 << " ms\n"
               << "updated rows so far: " << row_update_count << "\n"
               << "update rate: " << row_update_count/(to_nano_seconds(wait_end - thread_start)/1000000)*1000 << " updates/s\n";
            std::cout << ss.str() << std::flush;
            scan_sum = 0;
            scan_count = 0;
            wait_sum = 0;
        }
#endif
    }
}

void
Controller::_acquireSepLock(uint thread_id)
{
    ThreadStorage& thread_storage = *_thread_storages[thread_id];
    for (;;) {
        thread_storage.sep_is_reading_and_writing.store(true);
        if (!thread_storage.rta_is_ready_to_update_storage.load()) {
            break;
        }
        if (thread_storage.rta_is_ready_to_update_storage.load()){
            //unset SEP flag to avoid deadlock
            thread_storage.sep_is_reading_and_writing.store(false);
            while (thread_storage.rta_is_ready_to_update_storage.load()) {} //spin
        }
    }
}

void
Controller::_releaseSepLock(uint thread_id)
{
    _thread_storages[thread_id]->sep_is_reading_and_writing.store(false);
}
