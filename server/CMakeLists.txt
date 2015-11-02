project(server)
cmake_minimum_required(VERSION 2.8)

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/../cmake)

message(STATUS "Found ibverbs at ${Ibverbs_LIBRARY}")

find_package(Boost 1.47.0 REQUIRED COMPONENTS thread system timer)
include_directories(${Boost_INCLUDE_DIRS})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x -march=corei7-avx -pthread -fPIC -Winit-self -Wall -Wfatal-errors -Wpointer-arith -fno-omit-frame-pointer -pthread")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")

set(SQLITE_DB "../meta_db.db" CACHE STRING "SQLite database.")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/sep/sep_config.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/sep_config.h)

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

if(${_CMAKE_INSTRUMENTATION})
    add_definitions(-D_INSTRUMENTATION=${_CMAKE_INSTRUMENTATION})
endif(${_CMAKE_INSTRUMENTATION})

if(${_CMAKE_MERGE_INTERVAL})
    add_definitions(-D_MERGE_INTERVAL=${_CMAKE_MERGE_INTERVAL})
endif(${_CMAKE_MERGE_INTERVAL})

set(SEP_SRC
    sep/communication/AbstractSEPCommunication.cpp
    sep/communication/AbstractSEPCommunication.h
    sep/communication/TCPSEPCommunication.cpp
    sep/communication/TCPSEPCommunication.h
    sep/communication/InfinibandSEPCommunication.cpp
    sep/communication/InfinibandSEPCommunication.h
    sep/aim_column_finder.h
    sep/aim_schema.cpp
    sep/aim_schema.h
    sep/aim_schema_builder.cpp
    sep/aim_schema_builder.h
    sep/aim_schema_entry.cpp
    sep/aim_schema_entry.h
    sep/am_record.cpp
    sep/am_record.h
    sep/campaign.cpp
    sep/campaign.h
    sep/campaign_index.cpp
    sep/campaign_index.h
    sep/campaign_index_builder.cpp
    sep/campaign_index_builder.h
    sep/conjunct.cpp
    sep/conjunct.h
    sep/entry_attr_index.h
    sep/event.cpp
    sep/event.h
    sep/Math.h
    sep/OneDimRTree.h
    sep/predicate.cpp
    sep/predicate.h
    sep/schema_and_index_builder.cpp
    sep/schema_and_index_builder.h
    sep/sep.cpp
    sep/sep.h
    sep/type_descriptor.cpp
    sep/type_descriptor.h
    sep/utils.cpp
    sep/utils.h
    sep/value.cpp
    sep/value.h
    sep/window.cpp
    sep/window.h
)

set(RTA_SRC
    rta/communication/AbstractRTACommunication.h
    rta/communication/AbstractRTACommunication.cpp
    rta/communication/TCPRTACommunication.h
    rta/communication/TCPRTACommunication.cpp
    rta/communication/InfinibandRTACommunication.h
    rta/communication/InfinibandRTACommunication.cpp
    rta/Controller.cpp
    rta/Controller.h
    rta/dim_column_finder.h
    rta/dimension_record.cpp
    rta/dimension_record.h
    rta/dimension_schema.cpp
    rta/dimension_schema.h
    rta/utils.h
)

set(QUERY_SRC
    query/simd/queries.h
    query/AbstractQueryServerObject.cpp
    query/AbstractQueryServerObject.h
    query/Query1ServerObject.cpp
    query/Query1ServerObject.h
    query/Query2ServerObject.cpp
    query/Query2ServerObject.h
    query/Query3ServerObject.cpp
    query/Query3ServerObject.h
    query/Query4ServerObject.cpp
    query/Query4ServerObject.h
    query/Query5ServerObject.cpp
    query/Query5ServerObject.h
    query/Query6ServerObject.cpp
    query/Query6ServerObject.h
    query/Query7ServerObject.cpp
    query/Query7ServerObject.h
)

set(COLUMN_MAP_SRC
    column-map/ColumnMap.h
)

set(UTIL_SOURCE_DIR ${CMAKE_SOURCE_DIR}/../util)

set(UTIL_SRC
    ${UTIL_SOURCE_DIR}/logger.h
    ${UTIL_SOURCE_DIR}/serialization.h
    ${UTIL_SOURCE_DIR}/system-constants.h
    ${UTIL_SOURCE_DIR}/dimension-tables-mapping.h
    ${UTIL_SOURCE_DIR}/dimension-tables.h
    ${UTIL_SOURCE_DIR}/dimension-tables-unique-values.h
    ${UTIL_SOURCE_DIR}/dimension-tables-unique-values.cpp
    ${UTIL_SOURCE_DIR}/folly/ProducerConsumerQueue.h
    ${UTIL_SOURCE_DIR}/folly/ScopeGuard.h
    ${UTIL_SOURCE_DIR}/sqlite/sqlite3.h
    ${UTIL_SOURCE_DIR}/sqlite/sqlite3.c
)

include_directories(
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/..
    ${CMAKE_SOURCE_DIR}/../util
)

add_executable (aim
    ${SEP_SRC}
    ${RTA_SRC}
    ${QUERY_SRC}
    ${COLUMN_MAP_SRC}
    ${UTIL_SRC}
    main.cpp
)

target_link_libraries(aim ${Boost_LIBRARIES} dl)
