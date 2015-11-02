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

