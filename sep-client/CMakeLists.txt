project(sep-client)
cmake_minimum_required(VERSION 2.8)

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/../cmake)

find_package(Ibverbs REQUIRED)

find_package(Boost 1.47.0 REQUIRED COMPONENTS program_options system)
include_directories(${Boost_INCLUDE_DIRS})

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/../server   #only for tests
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../util
)

if(${_CMAKE_SERVER_NUM})
    add_definitions(-D_SERVER_NUM=${_CMAKE_SERVER_NUM})
endif(${_CMAKE_SERVER_NUM})

if(${_CMAKE_SEP_THREAD_NUM})
    add_definitions(-D_SEP_THREAD_NUM=${_CMAKE_SEP_THREAD_NUM})
endif(${_CMAKE_SEP_THREAD_NUM})

if(${_CMAKE_RTA_THREAD_NUM})
    add_definitions(-D_RTA_THREAD_NUM=${_CMAKE_RTA_THREAD_NUM})
endif(${_CMAKE_RTA_THREAD_NUM})

if(${_CMAKE_SUBSCR_NUM})
    add_definitions(-D_SUBSCR_NUM=${_CMAKE_SUBSCR_NUM})
endif(${_CMAKE_SUBSCR_NUM})

if(${_CMAKE_RECORDS_PER_BUCKET})
    add_definitions(-D_RECORDS_PER_BUCKET=${_CMAKE_RECORDS_PER_BUCKET})
endif(${_CMAKE_RECORDS_PER_BUCKET})

set(COMMUNICATION_SRC
    communication/AbstractSEPClientCommunication.h
    communication/AbstractSEPClientCommunication.cpp
    communication/TCPSEPClientCommunication.h
    communication/TCPSEPClientCommunication.cpp
    communication/InfinibandSEPClientCommunication.h
    communication/InfinibandSEPClientCommunication.cpp
)

set(UTIL_SOURCE_DIR ${CMAKE_SOURCE_DIR}/../util)

set(UTIL_SRC
    ${UTIL_SOURCE_DIR}/folly/ProducerConsumerQueue.h
    ${UTIL_SOURCE_DIR}/infiniband/inf_client.h
    ${UTIL_SOURCE_DIR}/logger.h
    ${UTIL_SOURCE_DIR}/serialization.h
    ${UTIL_SOURCE_DIR}/system-constants.h
)


add_executable(
    ${PROJECT_NAME}
    ${SRC_LIST}
    ${COMMUNICATION_SRC}
    ${UTIL_SRC}
    main.cpp
    SEPClient.h
    SEPClient.cpp
    # only for tests
    ../server/sep/event.h
    ../server/sep/event.cpp
    ../server/sep/communication/AbstractSEPCommunication.h
    ../server/sep/communication/AbstractSEPCommunication.cpp
    ../server/sep/communication/TCPSEPCommunication.h
    ../server/sep/communication/TCPSEPCommunication.cpp
    ../server/sep/communication/InfinibandSEPCommunication.h
    ../server/sep/communication/InfinibandSEPCommunication.cpp

)

target_link_libraries(
    ${PROJECT_NAME}
    ${Boost_LIBRARIES}
    ${Ibverbs_LIBRARY}
)

set(CMAKE_CXX_FLAGS "-std=c++0x -pthread ${CMAKE_CXX_FLAGS}")
