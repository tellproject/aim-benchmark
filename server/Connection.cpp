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

struct EventProcessor : public std::enable_shared_from_this<EventProcessor> {
private:
    boost::asio::io_service& mService;
    std::unique_ptr<tell::db::TransactionFiber<Context>> mFiber;
public:
    std::vector<Event> events;
    EventProcessor(boost::asio::io_service& service)
        : mService(service)
    {}
    void runTransaction(const tell::db::Transaction& tx, Context& context) {
        //for (const auto& event : events) {
        //    // process event
        //}
        mService.post([this]() {
            mFiber->wait();
            mFiber.reset(nullptr);
        });
    }
    void start(tell::db::ClientManager<Context>& clientManager) {
        auto fun = std::bind(&EventProcessor::runTransaction, shared_from_this(), std::placeholders::_1, std::placeholders::_2);
        mFiber.reset(new tell::db::TransactionFiber<Context>(clientManager.startTransaction(fun)));
    }
};

class CommandImpl {
    server::Server<CommandImpl> mServer;
    boost::asio::io_service& mService;
    tell::db::ClientManager<Context>& mClientManager;
    std::unique_ptr<tell::db::TransactionFiber<Context>> mFiber;
    std::vector<Event> mEventBatch;
    const AIMSchema &mAIMSchema;
    unsigned mEventBatchSize;
    Transactions mTransactions;
public:
    CommandImpl(boost::asio::ip::tcp::socket& socket,
            boost::asio::io_service& service,
            tell::db::ClientManager<Context>& clientManager,
            const AIMSchema &aimSchema,
            unsigned eventBatchSize)
        : mServer(*this, socket)
        , mService(service)
        , mClientManager(clientManager)
        , mAIMSchema(aimSchema)
        , mEventBatchSize(eventBatchSize)
        , mTransactions(aimSchema)
    {
        mEventBatch.reserve(mEventBatchSize);
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

            context.scanMemoryMananger = scanMemoryManager;

            context.subscriberId = tellSchema.idOf("subscriber_id");

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
        if (mEventBatch.size() >= mEventBatchSize) {
            // execute transaction
            auto processor = std::make_shared<EventProcessor>(mService);
            processor->events.swap(mEventBatch);
            mEventBatch.reserve(mEventBatchSize);
        }
        mEventBatch.push_back(args);
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
                unsigned eventBatchSize)
    : mSocket(service)
    , mImpl(new CommandImpl(mSocket, service, clientManager, aimSchema, eventBatchSize))
{}

Connection::~Connection() = default;

void Connection::run() {
    mImpl->run();
}

} // namespace aim

