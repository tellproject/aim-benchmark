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
#include <tuple>
#include <cstdint>
#include <type_traits>

#include <boost/system/error_code.hpp>
#include <boost/asio.hpp>
#include <boost/preprocessor.hpp>

#include <crossbow/Serializer.hpp>
#include <crossbow/string.hpp>

#define GEN_COMMANDS_ARR(Name, arr) enum class Name {\
    BOOST_PP_ARRAY_ELEM(0, arr) = 1, \
    BOOST_PP_ARRAY_ENUM(BOOST_PP_ARRAY_REMOVE(arr, 0)) \
} 

#define GEN_COMMANDS(Name, TUPLE) GEN_COMMANDS_ARR(Name, (BOOST_PP_TUPLE_SIZE(TUPLE), TUPLE))

#define EXPAND_CASE(r, t) case BOOST_PP_TUPLE_ELEM(2, 0, t)::BOOST_PP_ARRAY_ELEM(0, BOOST_PP_TUPLE_ELEM(2, 1, t)): \
    execute<BOOST_PP_TUPLE_ELEM(2, 0, t)::BOOST_PP_ARRAY_ELEM(0, BOOST_PP_TUPLE_ELEM(2, 1, t))>();\
    break;

#define SWITCH_PREDICATE(r, state) BOOST_PP_ARRAY_SIZE(BOOST_PP_TUPLE_ELEM(2, 1, state))

#define SWITCH_REMOVE_ELEM(r, state) (\
        BOOST_PP_TUPLE_ELEM(2, 0, state), \
        BOOST_PP_ARRAY_REMOVE(BOOST_PP_TUPLE_ELEM(2, 1, state), 0) \
        )\

#define SWITCH_CASE_IMPL(Name, Param, arr) switch (Param) {\
    BOOST_PP_FOR((Name, arr), SWITCH_PREDICATE, SWITCH_REMOVE_ELEM, EXPAND_CASE) \
}

#define SWITCH_CASE(Name, Param, t) SWITCH_CASE_IMPL(Name, Param, (BOOST_PP_TUPLE_SIZE(t), t))

namespace aim {

#define COMMANDS (POPULATE_TABLE, CREATE_SCHEMA, PROCESS_EVENT, Q1, Q2, Q3, Q4, Q5, Q6, Q7)

GEN_COMMANDS(Command, COMMANDS);

template<Command C>
struct Signature;

template<>
struct Signature<Command::POPULATE_TABLE> {
    using result = std::tuple<bool, crossbow::string>;
    using arguments = std::tuple<uint64_t /*lowestSubscriberNum*/, uint64_t /* highestSubscriberNum */>;
};

template<>
struct Signature<Command::CREATE_SCHEMA> {
    using result = std::tuple<bool, crossbow::string>;
    using arguments = void;
};

struct Event
{
    uint64_t call_id;
    uint64_t caller_id;
    uint64_t callee_id;
    double cost;
    uint64_t caller_place;
    uint64_t callee_place;
    int64_t timestamp;
    uint32_t duration;
    bool long_distance;
};

template<>
struct Signature<Command::PROCESS_EVENT> {
    using result = void;
    using arguments = Event;
};

/*
 * Q1: SELECT avg(total_duration_this_week)
 * FROM WT
 * WHERE number_of_local_calls_this_week > alpha;
 */

struct Q1In {
    uint32_t alpha;
};

struct Q1Out {
    using is_serializable = crossbow::is_serializable;
    bool success = true;
    crossbow::string error;
    double avg;

    template<class Archiver>
    void operator&(Archiver& ar) {
        ar & success;
        ar & error;
        ar & avg;
    }
};

template<>
struct Signature<Command::Q1> {
    using result = Q1Out;
    using arguments = Q1In;
};

/*
 * Q2: SELECT max(most_expensive_call_this_week)
 * FROM WT
 * WHERE number_of_calls_this_week > alpha;
 */

struct Q2In {
    uint32_t alpha;
};

struct Q2Out {
    using is_serializable = crossbow::is_serializable;
    bool success = true;
    crossbow::string error;
    double max;

    template<class Archiver>
    void operator&(Archiver& ar) {
        ar & success;
        ar & error;
        ar & max;
    }
};

template<>
struct Signature<Command::Q2> {
    using result = Q2Out;
    using arguments = Q2In;
};

/*
 * Q3: SELECT number_of_calls_this_week, sum(total_cost_this_week)/sum(total_duration_this_week) as cost_ratio
 * FROM WT
 * GROUP BY number_of_calls_this_week
 */

struct Q3Out {
    using is_serializable = crossbow::is_serializable;
    struct Q3Tuple {
        using is_serializable = crossbow::is_serializable;
        uint32_t number_of_calls_this_week;
        double cost_ratio;

        template<class Archiver>
        void operator&(Archiver& ar) {
            ar & cost_ratio;
            ar & number_of_calls_this_week;
        }
    };

    bool success = true;
    crossbow::string error;
    std::vector<Q3Tuple> results;

    template<class Archiver>
    void operator&(Archiver& ar) {
        ar & success;
        ar & error;
        ar & results;
    }
};

template<>
struct Signature<Command::Q3> {
    using result = Q3Out;
    using arguments = void;
};

/*
 * Q4:
 * SELECT city.name, avg(number of local calls this week), sum(total duration of local calls this week)
 * FROM WT, RegionInfo
 * WHERE number of local calls this week > alpha
 * AND total duration of local calls this week > beta
 * AND VWT.zip = RegionInfo.zip
 * GROUP BY city
 */

struct Q4In {
    uint32_t alpha;
    uint32_t beta;
};

struct Q4Out {
    using is_serializable = crossbow::is_serializable;
    struct Q4Tuple {
        using is_serializable = crossbow::is_serializable;
        crossbow::string city_name;
        double avg_num_local_calls_week;
        uint64_t sum_duration_local_calls_week;

        template<class Archiver>
        void operator&(Archiver& ar) {
            ar & city_name;
            ar & avg_num_local_calls_week;
            ar & sum_duration_local_calls_week;
        }
    };

    bool success = true;
    crossbow::string error;
    std::vector<Q4Tuple> results;

    template<class Archiver>
    void operator&(Archiver& ar) {
        ar & success;
        ar & error;
        ar & results;
    }
};

template<>
struct Signature<Command::Q4> {
    using result = Q4Out;
    using arguments = Q4In;
};

/*
 * Q5:
 * SELECT region.name, sum(total cost of local calls this week) as local,
 *        sum(total cost of long distance calls) as long_distance
 * FROM VWT, SubscriptionType t, SubscriberCategory c, RegionInfo r
 * WHERE t.type = subscriptionType AND c.type = subscriberCategory AND
 *       VWT.subscription-type = t.id AND VWT.category = c.id AND
 *       VWT.zip = r.zip
 * GROUP BY region
 */

struct Q5In {
    uint16_t sub_type;
    uint16_t sub_category;
};

struct Q5Out {
    using is_serializable = crossbow::is_serializable;
    struct Q5Tuple {
        using is_serializable = crossbow::is_serializable;
        crossbow::string region_name;
        double sum_cost_local_calls_week;
        double sum_cost_longdistance_calls_week;

        template<class Archiver>
        void operator&(Archiver& ar) {
            ar & region_name;
            ar & sum_cost_local_calls_week;
            ar & sum_cost_longdistance_calls_week;
        }
    };

    bool success = true;
    crossbow::string error;
    std::vector<Q5Tuple> results;

    template<class Archiver>
    void operator&(Archiver& ar) {
        ar & success;
        ar & error;
        ar & results;
    }
};

template<>
struct Signature<Command::Q5> {
    using result = Q5Out;
    using arguments = Q5In;
};

/*
 * Q6:
 * The goal of this query is to find the subscriber with
 * the longest call in this day and this week for local and
 * long distance calls. We are interested in subscribers
 * residing in a specific country and report subscriber-id
 * and length of the call. If several subscribers have the
 * max, we want to report the one with the lowest subscriber-id.
 */

struct Q6In {
    uint16_t country_id;
};

struct Q6Out {
    using is_serializable = crossbow::is_serializable;
    bool success = true;
    crossbow::string error;
    uint64_t max_local_week_id;
    int32_t max_local_week;
    uint64_t max_local_day_id;
    int32_t max_local_day;
    uint64_t max_distant_week_id;
    int32_t max_distant_week;
    uint64_t max_distant_day_id;
    int32_t max_distant_day;

    template<class Archiver>
    void operator&(Archiver& ar) {
        ar & max_local_week_id;
        ar & max_local_week;
        ar & max_local_day_id;
        ar & max_local_day;
        ar & max_distant_week_id;
        ar & max_distant_week;
        ar & max_distant_day_id;
        ar & max_distant_day;
    }
};

template<>
struct Signature<Command::Q6> {
    using result = Q6Out;
    using arguments = Q6In;
};

/*
 * Q7:
 * SELECT subscriber-id,
 *      min(avg cost of calls this T / avg duration of calls this T)
 *          as flat-rate
 * FROM VWT, SubscriberValue v
 * WHERE v.type = subscriberValueType
 * AND VWT.value = v.id
 *
 * T is either "week" or "day"
 */

struct Q7In {
    uint16_t subscriber_value_type;
    uint8_t window_length;  // 0: day, 1: week
};

struct Q7Out {
    using is_serializable = crossbow::is_serializable;
    bool success = true;
    crossbow::string error;
    uint64_t subscriber_id;
    double flat_rate;

    template<class Archiver>
    void operator&(Archiver& ar) {
        ar & success;
        ar & error;
        ar & subscriber_id;
        ar & flat_rate;
    }
};

template<>
struct Signature<Command::Q7> {
    using result = Q7Out;
    using arguments = Q7In;
};

namespace impl {

template<class... Args>
struct ArgSerializer;

template<class Head, class... Tail>
struct ArgSerializer<Head, Tail...> {
    ArgSerializer<Tail...> rest;

    template<class C>
    void exec(C& c, const Head& head, const Tail&... tail) const {
        c & head;
        rest.exec(c, tail...);
    }
};

template<>
struct ArgSerializer<> {
    template<class C>
    void exec(C&) const {}
};

}

namespace client {

template<class... Args>
struct argsType;
template<class A, class B, class... Tail>
struct argsType<A, B, Tail...> {
    using type = std::tuple<A, B, Tail...>;
};
template<class Arg>
struct argsType<Arg> {
    using type = Arg;
};
template<>
struct argsType<> {
    using type = void;
};

class CommandsImpl {
    boost::asio::ip::tcp::socket& mSocket;
    size_t mCurrSize = 1024;
    std::unique_ptr<uint8_t[]> mCurrentRequest;
public:
    CommandsImpl(boost::asio::ip::tcp::socket& socket)
        : mSocket(socket), mCurrentRequest(new uint8_t[mCurrSize])
    {
    }

    template<class Callback, class Result>
    typename std::enable_if<std::is_void<Result>::value, void>::type
    readResponse(const Callback& callback, size_t bytes_read = 0) {
        assert(bytes_read <= 1);
        if (bytes_read) {
            boost::system::error_code noError;
            callback(noError);
        }
        mSocket.async_read_some(boost::asio::buffer(mCurrentRequest.get(), mCurrSize),
                [this, callback, bytes_read](const boost::system::error_code& ec, size_t br){
                    if (ec) {
                        error<Result>(ec, callback);
                        return;
                    }
                    readResponse<Callback, Result>(callback, bytes_read + br);
                });
    }

    template<class Callback, class Result>
    typename std::enable_if<!std::is_void<Result>::value, void>::type
    readResponse(const Callback& callback, size_t bytes_read = 0) {
        auto respSize = *reinterpret_cast<size_t*>(mCurrentRequest.get());
        if (bytes_read >= 8 && respSize == bytes_read) {
            // response read
            Result res;
            boost::system::error_code noError;
            crossbow::deserializer ser(mCurrentRequest.get() + sizeof(size_t));
            ser & res;
            callback(noError, res);
            return;
        } else if (bytes_read >= 8 && respSize > mCurrSize) {
            std::unique_ptr<uint8_t[]> newBuf(new uint8_t[respSize]);
            memcpy(newBuf.get(), mCurrentRequest.get(), mCurrSize);
            mCurrentRequest.swap(newBuf);
        }
        mSocket.async_read_some(boost::asio::buffer(mCurrentRequest.get() + bytes_read, mCurrSize - bytes_read),
                [this, callback, bytes_read](const boost::system::error_code& ec, size_t br){
                    if (ec) {
                        Result res;
                        callback(ec, res);
                    }
                    readResponse<Callback, Result>(callback, bytes_read + br);
                });
    }

    template<class Res, class Callback>
    typename std::enable_if<std::is_void<Res>::value, void>::type
    error(const boost::system::error_code& ec, const Callback& callback) {
        callback(ec);
    }

    template<class Res, class Callback>
    typename std::enable_if<!std::is_void<Res>::value, void>::type
    error(const boost::system::error_code& ec, const Callback& callback) {
        Res res;
        callback(ec, res);
    }

    template<Command C, class Callback, class... Args>
    void execute(const Callback& callback, const Args&... args) {
        static_assert(
                (std::is_void<typename Signature<C>::arguments>::value && std::is_void<argsType<Args...>>::value) ||
                std::is_same<typename Signature<C>::arguments, typename argsType<Args...>::type>::value,
                "Wrong function arguments");
        using ResType = typename Signature<C>::result;
        crossbow::sizer sizer;
        sizer & sizer.size;
        sizer & C;
        impl::ArgSerializer<Args...> argSerializer;
        argSerializer.exec(sizer, args...);
        if (mCurrSize < sizer.size) {
            mCurrentRequest.reset(new uint8_t[sizer.size]);
            mCurrSize = sizer.size;
        }
        crossbow::serializer ser(mCurrentRequest.get());
        ser & sizer.size;
        ser & C;
        argSerializer.exec(ser, args...);
        ser.buffer.release();
        boost::asio::async_write(mSocket, boost::asio::buffer(mCurrentRequest.get(), sizer.size),
                    [this, callback](const boost::system::error_code& ec, size_t){
                        if (ec) {
                            error<ResType>(ec, callback);
                            return;
                        }
                        readResponse<Callback, ResType>(callback);
                    });
    }

};

} // namespace client

namespace server {

template<class Implementation>
class Server {
    Implementation& mImpl;
    boost::asio::ip::tcp::socket& mSocket;
    size_t mBufSize = 1024;
    std::unique_ptr<uint8_t[]> mBuffer;
    using error_code = boost::system::error_code;
public:
    Server(Implementation& impl, boost::asio::ip::tcp::socket& socket)
        : mImpl(impl)
        , mSocket(socket)
        , mBuffer(new uint8_t[mBufSize])
    {}
    void run() {
        read();
    }
private:
    template<Command C, class Callback>
    typename std::enable_if<std::is_void<typename Signature<C>::arguments>::value, void>::type
    execute(Callback callback) {
        mImpl.template execute<C>(callback);
    }

    template<Command C, class Callback>
    typename std::enable_if<!std::is_void<typename Signature<C>::arguments>::value, void>::type
    execute(Callback callback) {
        using Args = typename Signature<C>::arguments;
        Args args;
        crossbow::deserializer des(mBuffer.get() + sizeof(size_t) + sizeof(Command));
        des & args;
        mImpl.template execute<C>(args, callback);
    }

    template<Command C>
    typename std::enable_if<std::is_void<typename Signature<C>::result>::value, void>::type execute() {
        execute<C>([this]() {
            // send the result back
            mBuffer[0] = 1;
            boost::asio::async_write(mSocket,
                    boost::asio::buffer(mBuffer.get(), 1),
                    [this](const error_code& ec, size_t bytes_written) {
                        if (ec) {
                            std::cerr << ec.message() << std::endl;
                            return;
                        }
                        read(0);
                    }
            );
        });
    }

    template<Command C>
    typename std::enable_if<!std::is_void<typename Signature<C>::result>::value, void>::type execute() {
        using Res = typename Signature<C>::result;
        execute<C>([this](const Res& result) {
            // Serialize result
            crossbow::sizer sizer;
            sizer & sizer.size;
            sizer & result;
            if (mBufSize < sizer.size) {
                mBuffer.reset(new uint8_t[sizer.size]);
            }
            crossbow::serializer ser(mBuffer.get());
            ser & sizer.size;
            ser & result;
            ser.buffer.release();
            // send the result back
            boost::asio::async_write(mSocket,
                    boost::asio::buffer(mBuffer.get(), sizer.size),
                    [this](const error_code& ec, size_t bytes_written) {
                        if (ec) {
                            std::cerr << ec.message() << std::endl;
                            return;
                        }
                        read(0);
                    }
            );
        });
    }
    void read(size_t bytes_read = 0) {
        size_t reqSize = 0;
        if (bytes_read != 0) {
            reqSize = *reinterpret_cast<size_t*>(mBuffer.get());
        }
        if (bytes_read != 0 && reqSize == bytes_read) {
            // done reading
            auto cmd = *reinterpret_cast<Command*>(mBuffer.get() + sizeof(size_t));
            SWITCH_CASE(Command, cmd, COMMANDS)
            return;
        } else if (bytes_read >= 8 && reqSize > mBufSize) {
            std::unique_ptr<uint8_t[]> newBuf(new uint8_t[reqSize]);
            memcpy(newBuf.get(), mBuffer.get(), mBufSize);
            mBuffer.swap(newBuf);
        }
        mSocket.async_read_some(boost::asio::buffer(mBuffer.get() + bytes_read, mBufSize - bytes_read),
                [this, bytes_read](const error_code& ec, size_t br){
                    read(bytes_read + br);
                });
    }
};

} // namespace server

} // namespace aim

