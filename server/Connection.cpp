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
#include "Connection.hpp"
#include "CreateSchema.hpp"
#include "Populate.hpp"
#include "Transactions.hpp"

#include <telldb/Transaction.hpp>
#include <memory>

using namespace boost::asio;

namespace aim {

std::atomic<unsigned> processedEvents(0);

struct EventProcessor : public std::enable_shared_from_this<EventProcessor> {
private:
    boost::asio::io_service& mService;
    Transactions& mTransactions;
    tell::db::TransactionFiber<Context>* mFiber;
    std::atomic<bool>& mIsFree;
public:
    std::vector<Event> events;
    EventProcessor(boost::asio::io_service& service, Transactions& transactions, std::atomic<bool>& isFree)
        : mService(service)
        , mTransactions(transactions)
        , mIsFree(isFree)
    {}
    void runTransaction(tell::db::Transaction& tx, Context& context) {
        unsigned numEvents = events.size();
        mTransactions.processEvent(tx, context, events);
        processedEvents.fetch_add(numEvents);
        auto fiber = mFiber;
        auto isFree = &mIsFree;
        mService.post([fiber, isFree]() {
            isFree->store(true);
            fiber->wait();
            delete fiber;
        });
    }
    void start(tell::db::ClientManager<Context>& clientManager,
               size_t processingThread) {
        auto fun = std::bind(&EventProcessor::runTransaction, shared_from_this(),
                    std::placeholders::_1, std::placeholders::_2);
        mFiber = new tell::db::TransactionFiber<Context>(clientManager.startTransaction(
                    fun, tell::store::TransactionType::READ_WRITE, processingThread));
    }
};

void UdpServer::bind(const std::string& host, const std::string& port) {
    using namespace boost::asio;
    mSocket.open(ip::udp::v4());
    ip::udp::resolver res(mSocket.get_io_service());
    ip::udp::resolver::iterator iter;
    if (host == "") {
        iter = res.resolve(ip::udp::resolver::query(port));
    } else {
        iter = res.resolve(ip::udp::resolver::query(host, port));
    }
    decltype(iter) end;
    for (; iter != end; ++iter) {
        boost::system::error_code err;
        auto endpoint = iter->endpoint();
        mSocket.bind(endpoint, err);
        if (err) {
            LOG_WARN("Bind attempt failed " + err.message());
            continue;
        }
        break;
    }
    if (!mSocket.is_open()) {
        LOG_ERROR("Could not bind");
        std::terminate();
    }
}

void UdpServer::run() {
    listen();
    startTimer();
}

void UdpServer::startTimer() {
    mTimer.expires_from_now(std::chrono::seconds(1));
    mTimer.async_wait([this](const boost::system::error_code& ec){
        unsigned events = processedEvents.exchange(0);
        std::cout << events << std::endl;
        startTimer();
    });
}

void UdpServer::listen() {
    using err_code = boost::system::error_code;
    mSocket.async_receive(boost::asio::buffer(mBuffer.get(), mBufferSize), [this](const err_code& ec, size_t bt){
        if (ec) {
            LOG_ERROR(ec.message());
            run();
            return;
        }
#ifndef NDEBUG
        size_t reqSize = *reinterpret_cast<size_t*>(mBuffer.get());
        assert(reqSize == bt);
        auto cmd = *reinterpret_cast<Command*>(mBuffer.get() + sizeof(size_t));
        assert (cmd == Command::PROCESS_EVENT);
#endif
        crossbow::deserializer des(reinterpret_cast<uint8_t*>(mBuffer.get() + sizeof(size_t) + sizeof(Command)));
        Event ev;
        des & ev;
        size_t processingThread =
                ev.caller_id % mEventBatches.size();
        auto &eventBatch = mEventBatches[processingThread];
        auto isFree = mProcessingThreadFree[processingThread];
        if (eventBatch.size() >= mEventBatchSize && isFree->load()) {
            isFree->store(false);
            auto processor = std::make_shared<EventProcessor>(mSocket.get_io_service(), mTransactions, *isFree);
            processor->events.swap(eventBatch);
            eventBatch.reserve(mEventBatchSize);
            processor->start(mClientManager, processingThread);
        }
        if (eventBatch.size() < mEventBatchSize)
            eventBatch.push_back(ev);
        run();
    });
}

class CommandImpl {
    server::Server<CommandImpl> mServer;
    boost::asio::io_service& mService;
    tell::db::ClientManager<Context>& mClientManager;
    std::unique_ptr<tell::db::TransactionFiber<Context>> mFiber;
    const AIMSchema &mAIMSchema;
    Transactions mTransactions;
public:
    CommandImpl(boost::asio::ip::tcp::socket& socket,
            boost::asio::io_service& service,
            tell::db::ClientManager<Context>& clientManager,
            const AIMSchema &aimSchema)
        : mServer(*this, socket)
        , mService(service)
        , mClientManager(clientManager)
        , mAIMSchema(aimSchema)
        , mTransactions(aimSchema)
    {
    }

    void run() {
        mServer.run();
    }

    inline void initializeContextIfNecessary(tell::db::Transaction &tx, Context &context,
            const AIMSchema &aimSchema,
            tell::store::ScanMemoryManager *scanMemoryManager)
    {
        if (!context.isInitialized) {
            auto wFuture = tx.openTable("wt");
            auto wideTable = wFuture.get();
            auto tellSchema = tx.getSchema(wideTable);

            // initialize this vector
            context.tellIDToAIMSchemaEntry.reserve(aimSchema.numOfEntries());
            std::map<id_t, AIMSchemaEntry> tmpMap;

            for (unsigned i = 0; i < aimSchema.numOfEntries(); ++i) {
                tmpMap.emplace(std::make_pair(
                            tellSchema.idOf(aimSchema[i].name()),
                                    aimSchema[i]));
            }
            for (auto &pair: tmpMap)
                context.tellIDToAIMSchemaEntry.emplace_back(std::move(pair));

            context.scanMemoryMananger = scanMemoryManager;

            context.subscriberId = tellSchema.idOf("subscriber_id");
            context.timeStampId = tellSchema.idOf("last_updated");

            context.callsSumLocalWeek = tellSchema.idOf(aimSchema.getName(
                    Metric::CALL, AggrFun::SUM, FilterType::LOCAL, WindowLength::WEEK));
            context.callsSumAllWeek = tellSchema.idOf(aimSchema.getName(
                    Metric::CALL, AggrFun::SUM, FilterType::NO, WindowLength::WEEK));
            context.callsSumAllDay = tellSchema.idOf(aimSchema.getName(
                    Metric::CALL, AggrFun::SUM, FilterType::NO, WindowLength::DAY));

            context.durSumAllWeek = tellSchema.idOf(aimSchema.getName(
                    Metric::DUR, AggrFun::SUM,FilterType::NO, WindowLength::WEEK));
            context.durSumAllDay = tellSchema.idOf(aimSchema.getName(
                    Metric::DUR, AggrFun::SUM,FilterType::NO, WindowLength::DAY));
            context.durSumLocalWeek = tellSchema.idOf(aimSchema.getName(
                    Metric::DUR, AggrFun::SUM,FilterType::LOCAL, WindowLength::WEEK));

            context.durMaxLocalWeek = tellSchema.idOf(aimSchema.getName(
                    Metric::DUR, AggrFun::MAX,FilterType::LOCAL, WindowLength::WEEK));
            context.durMaxLocalDay = tellSchema.idOf(aimSchema.getName(
                    Metric::DUR, AggrFun::MAX,FilterType::LOCAL, WindowLength::DAY));
            context.durMaxDistantWeek = tellSchema.idOf(aimSchema.getName(
                    Metric::DUR, AggrFun::MAX,FilterType::NONLOCAL, WindowLength::WEEK));
            context.durMaxDistantDay = tellSchema.idOf(aimSchema.getName(
                    Metric::DUR, AggrFun::MAX,FilterType::NONLOCAL, WindowLength::DAY));

            context.costMaxAllWeek = tellSchema.idOf(aimSchema.getName(
                    Metric::COST, AggrFun::MAX, FilterType::NO, WindowLength::WEEK));
            context.costSumAllWeek = tellSchema.idOf(aimSchema.getName(
                    Metric::COST, AggrFun::SUM, FilterType::NO, WindowLength::WEEK));
            context.costSumAllDay = tellSchema.idOf(aimSchema.getName(
                    Metric::COST, AggrFun::SUM, FilterType::NO, WindowLength::DAY));
            context.costSumLocalWeek = tellSchema.idOf(aimSchema.getName(
                    Metric::COST, AggrFun::SUM, FilterType::LOCAL, WindowLength::WEEK));
            context.costSumDistantWeek = tellSchema.idOf(aimSchema.getName(
                    Metric::COST, AggrFun::SUM, FilterType::NONLOCAL, WindowLength::WEEK));

            context.subscriptionTypeId = tellSchema.idOf("subscription_type_id");

            context.regionZip = tellSchema.idOf("city_zip");
            context.regionCity = tellSchema.idOf("region_cty_id");
            context.regionCountry = tellSchema.idOf("region_country_id");
            context.regionRegion = tellSchema.idOf("region_region_id");

            context.valueTypeId = tellSchema.idOf("value_type_id");

            context.isInitialized = true;
        }
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::PROCESS_EVENT, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        LOG_ERROR("PROCESS_EVENT must be called over udp");
        std::terminate();
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::CREATE_SCHEMA, void>::type
    execute(const Callback callback) {
        auto transaction = [this, callback](tell::db::Transaction& tx, Context& context){
            bool success;
            crossbow::string msg;
            try {
                createSchema(tx, mAIMSchema);
                tx.commit();
                success = true;
            } catch (std::exception& ex) {
                std::cerr << ex.what() << std::endl;
                tx.rollback();
                success = false;
                msg = ex.what();
            }
            mService.post([this, callback, success, msg](){
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(std::make_tuple(success, msg));
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(mClientManager.startTransaction(transaction)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::POPULATE_TABLE, void>::type
    execute(std::tuple<uint64_t /*lowestSubscriberNum*/, uint64_t /* highestSubscriberNum */> args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            bool success;
            crossbow::string msg;
            try {
                Populator populator;
                populator.populateWideTable(tx, mAIMSchema, std::get<0>(args), std::get<1>(args));
                tx.commit();
                success = true;
            } catch (std::exception& ex) {
                tx.rollback();
                success = false;
                msg = ex.what();
            }
            mService.post([this, success, msg, callback](){
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(std::make_pair(success, msg));
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(mClientManager.startTransaction(transaction)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q1, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            initializeContextIfNecessary(tx, context,
                    mAIMSchema, mClientManager.getScanMemoryManager());
            typename Signature<C>::result res = mTransactions.q1Transaction(
                    tx, context, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(
                mClientManager.startTransaction(transaction,
                        tell::store::TransactionType::ANALYTICAL)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q2, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            initializeContextIfNecessary(tx, context,
                    mAIMSchema, mClientManager.getScanMemoryManager());
            typename Signature<C>::result res = mTransactions.q2Transaction(
                    tx, context, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(
                mClientManager.startTransaction(transaction,
                        tell::store::TransactionType::ANALYTICAL)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q3, void>::type
    execute(const Callback& callback) {
        auto transaction = [this, callback](tell::db::Transaction& tx, Context& context) {
            initializeContextIfNecessary(tx, context,
                    mAIMSchema, mClientManager.getScanMemoryManager());
            typename Signature<C>::result res = mTransactions.q3Transaction(
                    tx, context);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(
                mClientManager.startTransaction(transaction,
                        tell::store::TransactionType::ANALYTICAL)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q4, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            initializeContextIfNecessary(tx, context,
                    mAIMSchema, mClientManager.getScanMemoryManager());
            typename Signature<C>::result res = mTransactions.q4Transaction(
                    tx, context, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(
                mClientManager.startTransaction(transaction,
                        tell::store::TransactionType::ANALYTICAL)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q5, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            initializeContextIfNecessary(tx, context,
                    mAIMSchema, mClientManager.getScanMemoryManager());
            typename Signature<C>::result res = mTransactions.q5Transaction(
                    tx, context, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(
                mClientManager.startTransaction(transaction,
                        tell::store::TransactionType::ANALYTICAL)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q6, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            initializeContextIfNecessary(tx, context,
                    mAIMSchema, mClientManager.getScanMemoryManager());
            typename Signature<C>::result res = mTransactions.q6Transaction(
                    tx, context, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(
                mClientManager.startTransaction(transaction,
                        tell::store::TransactionType::ANALYTICAL)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q7, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            initializeContextIfNecessary(tx, context,
                    mAIMSchema, mClientManager.getScanMemoryManager());
            typename Signature<C>::result res = mTransactions.q7Transaction(
                    tx, context, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(
                mClientManager.startTransaction(transaction,
                        tell::store::TransactionType::ANALYTICAL)));
    }

};

Connection::Connection(boost::asio::io_service& service,
                tell::db::ClientManager<Context>& clientManager,
                const AIMSchema &aimSchema,
                size_t processingThreads,
                unsigned eventBatchSize)
    : mSocket(service)
    , mImpl(new CommandImpl(mSocket, service, clientManager, aimSchema))
{}

Connection::~Connection() = default;

void Connection::run() {
    mImpl->run();
}

} // namespace aim

