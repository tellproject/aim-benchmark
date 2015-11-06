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

struct EventProcessor  : public std::enable_shared_from_this<EventProcessor> {
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
    unsigned mEventBatchSize;
    Transactions mTransactions;
    const AIMSchema &mAIMSchema;
    const DimensionSchema &mDimensionSchema;
public:
    CommandImpl(boost::asio::ip::tcp::socket& socket,
            boost::asio::io_service& service,
            tell::db::ClientManager<Context>& clientManager,
            const AIMSchema &aimSchema,
            const DimensionSchema &dimensionSchema,
            unsigned eventBatchSize)
        : mServer(*this, socket)
        , mService(service)
        , mClientManager(clientManager)
        , mEventBatchSize(eventBatchSize)
        , mAIMSchema(aimSchema)
        , mDimensionSchema(dimensionSchema)
    {
        mEventBatch.reserve(mEventBatchSize);
    }

    void run() {
        mServer.run();
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
           /* bool success;
            crossbow::string msg;
            try {
                createSchema(tx);
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
            })*/;
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
                populator.populateWideTable(tx, mAIMSchema, mDimensionSchema, std::get<0>(args), std::get<1>(args));
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
            typename Signature<C>::result res = mTransactions.q1Transaction(tx, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(mClientManager.startTransaction(transaction)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q2, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            typename Signature<C>::result res = mTransactions.q2Transaction(tx, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(mClientManager.startTransaction(transaction)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q3, void>::type
    execute(const Callback& callback) {
        auto transaction = [this, callback](tell::db::Transaction& tx, Context& context) {
            typename Signature<C>::result res = mTransactions.q3Transaction(tx);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(mClientManager.startTransaction(transaction)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q4, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            typename Signature<C>::result res = mTransactions.q4Transaction(tx, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(mClientManager.startTransaction(transaction)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q5, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            typename Signature<C>::result res = mTransactions.q5Transaction(tx, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(mClientManager.startTransaction(transaction)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q6, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            typename Signature<C>::result res = mTransactions.q6Transaction(tx, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(mClientManager.startTransaction(transaction)));
    }

    template<Command C, class Callback>
    typename std::enable_if<C == Command::Q7, void>::type
    execute(const typename Signature<C>::arguments& args, const Callback& callback) {
        auto transaction = [this, args, callback](tell::db::Transaction& tx, Context& context) {
            typename Signature<C>::result res = mTransactions.q7Transaction(tx, args);
            mService.post([this, res, callback]() {
                mFiber->wait();
                mFiber.reset(nullptr);
                callback(res);
            });
        };
        mFiber.reset(new tell::db::TransactionFiber<Context>(mClientManager.startTransaction(transaction)));
    }

};

Connection::Connection(boost::asio::io_service& service,
                tell::db::ClientManager<Context>& clientManager,
               const AIMSchema &aimSchema,
               const DimensionSchema &dimensionSchema,
               unsigned eventBatchSize)
    : mSocket(service)
    , mImpl(new CommandImpl(mSocket, service, clientManager, aimSchema, dimensionSchema, eventBatchSize))
{}

Connection::~Connection() = default;

void Connection::run() {
    mImpl->run();
}

} // namespace aim

