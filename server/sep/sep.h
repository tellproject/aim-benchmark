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

#include "rta/Controller.h"
#include "rta/dimension_record.h"
#include "sep/campaign_index.h"
#include "sep/communication/AbstractSEPCommunication.h"

struct AtomicStats
{
    AtomicStats() = default;
    ~AtomicStats() = default;
    /*
     * Initializations of atomic values.
     */
    void init() {
        sum = max = 0;
        cnt = gr_0_1 = gr_0_2 = gr_0_3 = gr_0_4
            = gr_0_5 = gr_1 = gr_5 = gr_10 = 0;
    }
    std::atomic<double> sum, max;
    std::atomic<ulong> cnt, gr_0_1, gr_0_2, gr_0_3, gr_0_4;
    std::atomic<ulong> gr_0_5, gr_1, gr_5, gr_10;
};

/*
 * This class brings together the various components of the benchmark. Given
 * the Campaign Index and the Schema of the AM table it executes the SEP benchmark.
 * A bunch of statistical information is maintained and is reported. Given that
 * multiple threads running concurrently we use atomic variables to ensure that
 * updates of statistics are executed atomically.
 */
class Sep
{
public:
    struct Schemas {
        const AIMSchema& aim;
        const DimensionSchema& dimension;
    };

    typedef std::default_random_engine Engine;

    Sep(uint sep_server_id, const AIMSchema& aim_schema,
        const DimensionSchema& dim_schema,
        const CampaignIndex& campaign_index,
        std::unique_ptr<AbstractRTACommunication> rta_module,
        std::unique_ptr<AbstractSEPCommunication> sep_module,
        bool start_workers = true);

    ~Sep() = default;

    /*
     * It spawns a number of threads, each of which is responsible
     * for serving an event.
     */
    int execute();

public://for testing purposes and standalone-client
    void populate();

    void processEvents();

    void stop();

    Controller& controller() {return _controller;}

    Schemas schemas() { return _schemas; }

    void enqueueUpdates(const std::vector<Event>& events)
    {
        auto queues_size = _sep_server_module->queuesSize();
        for (const auto& event: events) {
            auto queue_id = event.callerId() % queues_size;
            _sep_server_module->__queueEvent(queue_id, event);
        }
    }

    void processAllQueuedEvents(uint thread_id);

private:
    static constexpr std::size_t _sep_thread_num = SEP_THREAD_NUM;
    static constexpr ulong _subscribers = SUBSCR_NUM;
    static constexpr ulong _event_num = EVENTS;
    std::atomic_bool _running;
    bool _populated;
    std::vector<std::thread> _workers;
    uint _sep_server_id;
    Schemas _schemas;
    const CampaignIndex& _campaign_index;
    std::atomic<ulong> _events_processed;
    AtomicStats _stats;
    std::atomic<ulong> _sub_init;
    std::atomic<ulong> _population_cnt;
    Controller _controller;
    std::unique_ptr<AbstractSEPCommunication> _sep_server_module;

private:
    /*
     * Spawns a number of populator threads responsible for inserting dummy
     * (initial) records into the database table. It waits for threads to
     * join. Only does something when called for the first time. Any subsequent
     * call just returns. Returns 0 on success, 1 otherwise.
     */
    int _populateTable();

    /*
     * Given an AM record and a Dimension record it builds a vector that
     * concatenates the two records into a merged one. We only use it
     * for initialization.
     */
    std::vector<char> _fullRecord(const AMRecord&, const DimensionRecord&);

    /*
     * This function inserts initial records into the database.
     */
    void _initialize(const Timestamp start);

    /*
     * It runs infinitely. It pops an event from the communication queue and
     * it serves it. For doing so it invokes updateAM() and probeIndex(). Here,
     * we measure the performance of the updates.
     */
    void _processEvents(uint thread_id);

    /*
     * Process a single event -> update AM and probe index.
     */
    ulong _processEvent(const Event &event);

    /*
     * It probes the campaign index using the updated subscriber profile and
     * returns a boost bitset. There, one bit per campaign exists. This
     * bit is set if the campaign has triggered.
     */
    boost::dynamic_bitset<> _probeIndex(const AMRecord&) const;

    /*
     * This function is entitled to read and write information to the storage
     * layer. More specifically, it reads the record of a subsciber, it
     * updates it and finally it writes back the new record.
     */
    AMRecord _updateAM(const Event&);

    /*
     * It prints the statistical information we gather.
     */
    void _printStatistics();

    /*
     * This function updates the statistics we maintain.
     */
    void _updateStatistics(double latency);

    /*
     * Prints the id of the sep server.
     */
    uint _sepServerId() const;
};
