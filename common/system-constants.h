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

#include <sys/types.h>
#include <cstdint>
#include <string>

// used for RTA communication
constexpr uint8_t NUM_QUERY_TYPES = 7;
constexpr uint RTA_REQUEST_BUFFER_SIZE = 32;
constexpr uint RTA_RESPONSE_BUFFER_SIZE = 10*4*4096;             //16 KB
constexpr uint8_t GET_NEXT_RESULT_SYMBOL = 255;
constexpr uint32_t RTA_PORT = 5001;

// used for SEP communication
constexpr uint32_t SEP_PORT = 6001;

#ifndef _RECORDS_PER_BUCKET
static_assert(false, "_RECORDS_PER_BUCKET not specified");
#endif

#ifndef _SERVER_NUM
static_assert(false, "_SERVER_NUM not specified");
#endif

#ifndef _SEP_THREAD_NUM
static_assert(false, "_SEP_THREAD_NUM not specified");
#endif

#ifndef _RTA_THREAD_NUM
static_assert(false, "_RTA_THREAD_NUM NUM not specified");
#endif

#ifndef _SUBSCR_NUM
static_assert(false, "_SUBSCR_NUM NUM not specified");
#endif

#ifdef _INSTRUMENTATION
    #define INSTRUMENTATION
#endif

// only used for differential updates
#ifndef _MERGE_INTERVAL
    #define _MERGE_INTERVAL 1
#endif
constexpr uint64_t MERGE_INTERVAL = _MERGE_INTERVAL;

// used for column map, default value =3*1024
constexpr uint64_t RECORDS_PER_BUCKET = _RECORDS_PER_BUCKET;

constexpr size_t SERVER_NUM = _SERVER_NUM;

// used in server
constexpr size_t SEP_THREAD_NUM = _SEP_THREAD_NUM;
constexpr size_t RTA_THREAD_NUM = _RTA_THREAD_NUM;
static_assert(RTA_THREAD_NUM % SEP_THREAD_NUM == 0,
              "RTA_THREAD_NUM must be a multiple of SEP_THREAD_NUM");

// constexpr ulong SUBSCR_NUM = 10*1024*1024;
constexpr ulong SUBSCR_NUM = _SUBSCR_NUM;

constexpr ulong EVENTS = 10*1024 * SUBSCR_NUM;
constexpr ulong TIME_SPAN = 2592000000;     //a month in msecs
constexpr uint SEP_SERVER_QUEUES_SIZE = 1000;
constexpr ulong DELTA_ENTRY_ALLOC_HINT = 100 * MERGE_INTERVAL * SUBSCR_NUM / (10*1000*1000) / RTA_THREAD_NUM;   // just a rough idea, may have to be played around with a little...

// common structs and enums for communication
enum class NetworkProtocol: uint8_t {TCP, Infiniband};

struct ServerAddress
{
    ServerAddress(std::string host, uint32_t port):
          host(host), port(port) {}

    std::string host;
    uint32_t port;
};
