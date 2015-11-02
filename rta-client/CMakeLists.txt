project(rta-client)
cmake_minimum_required(VERSION 2.8)

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/../cmake)

find_package(Ibverbs REQUIRED)

find_package(Boost 1.47.0 REQUIRED COMPONENTS program_options system)
include_directories(${Boost_INCLUDE_DIRS})

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
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

set(QUERY_SRC
    query/AbstractQueryClientObject.h
    query/AbstractQueryClientObject.cpp
    query/Query1ClientObject.h
    query/Query1ClientObject.cpp
    query/Query2ClientObject.h
    query/Query2ClientObject.cpp
    query/Query3ClientObject.h
    query/Query3ClientObject.cpp
    query/Query4ClientObject.h
    query/Query4ClientObject.cpp
    query/Query5ClientObject.h
    query/Query5ClientObject.cpp
    query/Query6ClientObject.h
    query/Query6ClientObject.cpp
    query/Query7ClientObject.h
    query/Query7ClientObject.cpp
)

set(COMMUNICATION_SRC
    communication/AbstractRTAClientCommunication.h
    communication/AbstractRTAClientCommunication.cpp
    communication/TCPRTAClientCommunication.h
    communication/TCPRTAClientCommunication.cpp
    communication/InfinibandRTAClientCommunication.h
    communication/InfinibandRTAClientCommunication.cpp
)

set(UTIL_SOURCE_DIR ${CMAKE_SOURCE_DIR}/../util)

set(UTIL_SRC
    ${UTIL_SOURCE_DIR}/infiniband
    ${UTIL_SOURCE_DIR}/logger.h
    ${UTIL_SOURCE_DIR}/serialization.h
    ${UTIL_SOURCE_DIR}/system-constants.h
    ${UTIL_SOURCE_DIR}/dimension-tables-mapping.h
    ${UTIL_SOURCE_DIR}/dimension-tables.h
    ${UTIL_SOURCE_DIR}/dimension-tables-unique-values.h
    ${UTIL_SOURCE_DIR}/dimension-tables-unique-values.cpp
)

add_executable(
    ${PROJECT_NAME}
    ${SRC_LIST}
    ${COMMUNICATION_SRC}
    ${QUERY_SRC}
    ${UTIL_SRC}
    main.cpp
    RTAClient.h
    RTAClient.cpp
)

target_link_libraries(
    ${PROJECT_NAME}
    ${Boost_LIBRARIES}
    ${Ibverbs_LIBRARY}
)

set(CMAKE_CXX_FLAGS "-std=c++0x -pthread ${CMAKE_CXX_FLAGS}")
