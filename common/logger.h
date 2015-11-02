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

//##### comment out the following lines if logging should be turned off #####/
#define LOG_ERRORS      // should actually always be turned on
#define LOG_WARNINGS    // can help when debugging
//#define LOG_INFOS       // displays a lot of information necessary for testing

//#### do not change anything below this line ####//

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <errno.h>
#include <mutex>

static std::mutex s_logMutex;

#ifdef LOG_ERRORS
    #define LOG_ERROR(Y) { std::lock_guard<std::mutex> lock(s_logMutex);\
    std::cout << "[ERROR:] " << Y << ". Error cause:" << strerror(errno)\
    << std::endl; }
#else
    #define LOG_ERROR(Y)
#endif

#ifdef LOG_WARNINGS
    #define LOG_WARNING(Y) { std::lock_guard<std::mutex> lock(s_logMutex);\
    std::cout << "[WARNING:] " << Y << "." << std::endl; }
#else
    #define LOG_WARNING(Y)
#endif

#ifdef LOG_INFOS
    #define LOG_INFO(Y) { std::lock_guard<std::mutex> lock(s_logMutex);\
    std::cout << "[INFO:] " << Y << "." << std::endl; }
#else
    #define LOG_INFO(Y)
#endif

template<typename Duration>
double to_nano_seconds(const Duration& dur) {
    return double(std::chrono::duration_cast<std::chrono::nanoseconds>(dur).count());
}

