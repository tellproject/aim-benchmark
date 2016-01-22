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
//#include "sep.h"

//#include <boost/dynamic_bitset.hpp>
//#include <cassert>
//#include <iostream>
//#include <random>
//#include <string>
//#include <sstream>

//#include "common/logger.h"
//#include "server/rta/dimension_record.h"

//using namespace std::chrono;

///*
// * The system collects statistical information for periods of time.
// */
//std::chrono::time_point<std::chrono::system_clock> period_start, period_end;

///*
// * Constructor that initializes all the statistical information. It also
// * computes the timestamp of the very first event. For all the future events
// * we add a constant to this time.
// */
//Sep::Sep(uint sep_server_id,
//         const AIMSchema& aim_schema,
//         const DimensionSchema& dim_schema,
//         const CampaignIndex& campaign_index,
//         std::unique_ptr<AbstractRTACommunication> rta_module,
//         std::unique_ptr<AbstractSEPCommunication> sep_module,
//         bool start_workers):
//    _running(true),
//    _populated(false),
//    _sep_server_id(sep_server_id),
//    _schemas{aim_schema, dim_schema},
//    _campaign_index(campaign_index),
//    _events_processed(0), _sub_init(1), _population_cnt(0),
//    _controller(aim_schema, dim_schema, std::move(rta_module), start_workers),
//    _sep_server_module(std::move(sep_module))
//{
//    _stats.init();
//}

///*
// * It spawns a number of threads each of which generates and serves events.
// * returns 1 if there was a problem with population and 0 otherwise.
// */
//int
//Sep::execute()
//{
//    if (_populateTable())
//          return 1;

//    for (size_t i=0; i<_sep_thread_num; ++i) {
//        _workers.emplace_back(&Sep::_processEvents, this, i);
//    }
//    for (std::thread &worker: _workers) {
//        worker.join();
//    }
//    return 0;
//}

//void Sep::populate()
//{
//    _populateTable();
//}

//int
//Sep::_populateTable()
//{
//    if (_populated) {
//        return 0;
//    }
//    uint size = 2*findAvailableCores();
//    std::vector<std::thread> populators;
//    populators.reserve(size);

//    std::cout << "Create " << size << " populators!" << std::endl;
//    auto dur = system_clock::now().time_since_epoch();
//    Timestamp dummy_time = duration_cast<milliseconds>(dur).count();

//    auto population_start = std::chrono::high_resolution_clock::now();
//    for (uint i=0; i<size; ++i) {
//        populators.emplace_back(&Sep::_initialize, this, dummy_time);
//    }
//    for (auto &populator: populators) {
//        populator.join();
//    }
//    std::cout << "Populators joined!" << std::endl;
//    auto population_end = std::chrono::high_resolution_clock::now();
//    std::cout << "Initialization done with " << _population_cnt
//              << " subscribers for server " << _sep_server_id << " in "
//              << duration_cast<seconds>(population_end - population_start).count()
//              << " secs" << endl;
//    _populated = true;
//    return 0;
//}

//std::vector<char>
//Sep::_fullRecord(const AMRecord &aim_rec, const DimensionRecord &dim_rec)
//{
//    std::vector<char> full(aim_rec.data().size() + dim_rec.data().size());

//    char *tmp = full.data();
//    memcpy(tmp, aim_rec.data().data(), aim_rec.data().size());
//    tmp += aim_rec.data().size();

//    memcpy(tmp, dim_rec.data().data(), dim_rec.data().size());
//    return std::move(full);
//}

///*
// * Function that generates a default record for every subscriber. For all the
// * AM attributes the init default function is invoked. All the records have the
// * same default timestamp (current time of system = time when the constructor
// * of the class is called).
// */
//void
//Sep::_initialize(const Timestamp start)
//{
//    uint64_t cur_sub = 1;
//    std::mt19937 eng;
//    std::uniform_int_distribution<uint> distr(0, 10);
//    while ((cur_sub = _sub_init.fetch_add(1)) <= _subscribers) {
//        if (cur_sub % (128*1024) == 0) {
//            std::cout << cur_sub * 100 / _subscribers << "% done" << std::endl;
//        }
//        /*
//         * We store only subscribers whose events will be sent to this server.
//         * Not all servers are responsible for all subscribers' events. We use
//         * the same formula [(sub_id >> 8) % SERVER_NUM] we use on the SEP
//         * client side. We construct a record that consists of the aim part
//         * and the dimensions part. We call it full_rec.
//         */
//        if (belongToThisServer(cur_sub, _sep_server_id)) {
//            AMRecord aim_rec(_schemas.aim, start);
//            DimensionRecord dim_rec(cur_sub, _schemas.dimension, eng, distr);
//            _controller.populate(cur_sub, _fullRecord(aim_rec, dim_rec).data());
//            _population_cnt++;
//        }
//    }
//    return;
//}

//void
//Sep::processEvents()
//{
//    for (size_t i=0; i<_sep_thread_num; ++i) {
//        _workers.emplace_back(&Sep::_processEvents, this, i);
//    }
//    for (std::thread &worker: _workers) {
//        worker.join();
//    }
//}

//void
//Sep::stop()
//{
//    _running = false;
//}

//void
//Sep::processAllQueuedEvents(uint thread_id)
//{
//    std::chrono::time_point<std::chrono::system_clock> cur_start, cur_end;
//    period_start = std::chrono::high_resolution_clock::now();
//    ulong related_camps = 0, cur_event = 0;
//    Event* event;
//    while ((event = _sep_server_module->frontPtr(thread_id)) != nullptr) {
//        cur_event = _events_processed.fetch_add(1);
//        cur_start = std::chrono::high_resolution_clock::now();

//        related_camps += _processEvent(*event);
//        _sep_server_module->popFront(thread_id);

//        cur_end = std::chrono::high_resolution_clock::now();
//        _updateStatistics(to_nano_seconds(cur_end - cur_start));

////        if (((cur_event - 1) % (100 * 1024)) == 0) {
////            std::cout << "-----------------------------------------------\n";
////            std::cout << "\n" << cur_event * 100 / _event_num << "% done\n"
////                      << "cur: " << cur_event << " total: " << _event_num
////                      << "\ncamps: " << related_camps << std::endl;
////            _printStatistics();
////            period_start = std::chrono::high_resolution_clock::now();
////        }
//    }
//}

///*
// * This function pops an event from the queue belonging to thread with id
// * 'thread_id'. Based on that event it updates the AM and it evaluates the
// * active campaigns.
// */
//void
//Sep::_processEvents(uint thread_id)
//{
//    while (_running) {
//       processAllQueuedEvents(thread_id);
//    }
//}

///*
// * Process the incoming event:
// * 1) updates the AM table
// * 2) evaluates the existing campaigns
// */
//ulong
//Sep::_processEvent(const Event &event)
//{
//    AMRecord am_record = _updateAM(event);
//    boost::dynamic_bitset<> bitset = _probeIndex(am_record);
//    return static_cast<ulong>(bitset.count());
//}

///*
// * This funtion probes the campaign index using the updated subscriber profile.
// */
//boost::dynamic_bitset<>
//Sep::_probeIndex(const AMRecord &record) const
//{
//    auto camp_bitset = _campaign_index.matchCampaigns(record);
//    return std::move(camp_bitset);
//}

///*
// * The function is entitled for updating a subscriber's profile. It reads
// * his/her record, it generates an updated record based on the record it
// * retrieved and the current event and finally it writes the newly created
// * record back to the storage layer.
// */
//AMRecord
//Sep::_updateAM(const Event &event)
//{
//    AMRecord am_record;
//    std::vector<char> record;

//    ulong key = event.callerId();
//    auto check = _controller.read(key, &record);

//    /*
//     * There is no previous record for this subscriber.
//     */
//    if (check == Controller::ReadResult::DoesntExist) { //write a new record
//        am_record = AMRecord(_schemas.aim, event);
//    }
//    /*
//     * Subscriber has generated other events in the past. Retrieve his/her
//     * previous record and create a new one.
//     */
//    else if (check == Controller::ReadResult::Exists) { //record exists
//        am_record = AMRecord(record.data(), _schemas.aim, event);
//    }
//    else {					 //error case
//        assert(false);
//        return decltype(_updateAM(event))();
//    }

//    auto vec = am_record.data();
//    _controller.write(key, std::move(vec));

//    return std::move(am_record);
//}

///*
// * Function printing the aggregated statistics for a specific time period
// * (called once per a number of events). At the same time we reset the
// * values (i.e. .exchange(0)) we obtained preparing this way the statistics for
// * the next measurement period.
// */
//void
//Sep::_printStatistics()
//{
//    period_end = std::chrono::high_resolution_clock::now();

//    auto dur = to_nano_seconds(period_end - period_start);

//    double cnt = double(_stats.cnt.exchange(0));
//    std::stringstream s;
//    s << "Cnt: " << cnt << std::endl;
//    s << "Avg: " << (_stats.sum.exchange(0) / cnt) / 1000000 << " ms" << std::endl;
//    s << "Max: " << _stats.max.exchange(0) / 1000000 << " ms" << std::endl;
//    s << "Tp: " << cnt / (dur/1000000000) << std::endl;
//    s << "Gr_0.1: " << double(_stats.gr_0_1.exchange(0))/cnt << std::endl;
//    s << "Gr_0.2: " << double(_stats.gr_0_2.exchange(0))/cnt << std::endl;
//    s << "Gr_0.3: " << double(_stats.gr_0_3.exchange(0))/cnt << std::endl;
//    s << "Gr_0.4: " << double(_stats.gr_0_4.exchange(0))/cnt << std::endl;
//    s << "Gr_0.5: " << double(_stats.gr_0_5.exchange(0))/cnt << std::endl;
//    s << "Gr_1: " << double(_stats.gr_1.exchange(0))/cnt << std::endl;
//    s << "Gr_5: " << double(_stats.gr_5.exchange(0))/cnt << std::endl;
//    s << "Gr_10: " << double(_stats.gr_10.exchange(0))/cnt << std::endl;
//    s << "\n\n" << std::endl;
//    std::cout << s.str();
//}

///*
// * Function responsible for updating the statistics after the processing of an
// * event. We use atomic variables for ensuring atomic updates.
// */
//void
//Sep::_updateStatistics(double latency)
//{
//    double old_sum = _stats.sum;
//       while (_stats.sum.compare_exchange_strong(old_sum, old_sum + latency)) {}
//       _stats.cnt.fetch_add(1);
//       if (latency > 100000)
//           _stats.gr_0_1.fetch_add(1);
//       if (latency > 200000)
//           _stats.gr_0_2.fetch_add(1);
//       if (latency > 300000)
//           _stats.gr_0_3.fetch_add(1);
//       if (latency > 400000)
//           _stats.gr_0_4.fetch_add(1);
//       if (latency > 500000)
//           _stats.gr_0_5.fetch_add(1);
//       if (latency > 1000000)
//           _stats.gr_1.fetch_add(1);
//       if (latency > 5000000)
//           _stats.gr_5.fetch_add(1);
//       if (latency > 10000000)
//           _stats.gr_10.fetch_add(1);

//       double old_max = _stats.max;
//       while (latency > old_max && !_stats.max.compare_exchange_strong(old_max, latency)) {}
//}

//uint
//Sep::_sepServerId() const
//{
//    return _sep_server_id;
//}
