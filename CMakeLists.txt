cmake_minimum_required(VERSION 3.0.2)
project(aim-benchmark)

find_package(Boost REQUIRED COMPONENTS system)
find_package(TellDB REQUIRED)
find_package(Crossbow REQUIRED)

set(COMMON_SRC
    common/Protocol.cpp
    common/Util.cpp
    common/serialization.h
    common/dimension-tables-mapping.h
    common/dimension-tables.h
    common/dimension-tables-unique-values.h
    common/dimension-tables-unique-values.cpp
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mcx16 -ftemplate-depth=512")

add_library(aim_common STATIC ${COMMON_SRC})
# Workaround for link failure with GCC 5 (GCC Bug 65913)
#if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
#    target_link_libraries(aim_common PUBLIC atomic)
#endif()
target_include_directories(aim_common PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories(aim_common PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(aim_common PUBLIC ${Boost_LIBRARIES})
target_link_libraries(aim_common PUBLIC telldb)

set(SERVER_SRC
    server/main.cpp
    server/sep/communication/AbstractSEPCommunication.cpp
    server/sep/communication/AbstractSEPCommunication.h
    server/sep/communication/TCPSEPCommunication.cpp
    server/sep/communication/TCPSEPCommunication.h
    server/sep/communication/InfinibandSEPCommunication.cpp
    server/sep/communication/InfinibandSEPCommunication.h
    server/sep/aim_column_finder.h
    server/sep/aim_schema.cpp
    server/sep/aim_schema.h
    server/sep/aim_schema_builder.cpp
    server/sep/aim_schema_builder.h
    server/sep/aim_schema_entry.cpp
    server/sep/aim_schema_entry.h
    server/sep/am_record.cpp
    server/sep/am_record.h
    server/sep/campaign.cpp
    server/sep/campaign.h
    server/sep/campaign_index.cpp
    server/sep/campaign_index.h
    server/sep/campaign_index_builder.cpp
    server/sep/campaign_index_builder.h
    server/sep/conjunct.cpp
    server/sep/conjunct.h
    server/sep/entry_attr_index.h
    server/sep/event.cpp
    server/sep/event.h
    server/sep/Math.h
    server/sep/OneDimRTree.h
    server/sep/predicate.cpp
    server/sep/predicate.h
    server/sep/schema_and_index_builder.cpp
    server/sep/schema_and_index_builder.h
    server/sep/sep.cpp
    server/sep/sep.h
    server/sep/type_descriptor.cpp
    server/sep/type_descriptor.h
    server/sep/utils.cpp
    server/sep/utils.h
    server/sep/value.cpp
    server/sep/value.h
    server/sep/window.cpp
    server/sep/window.h
    server/rta/communication/AbstractRTACommunication.h
    server/rta/communication/AbstractRTACommunication.cpp
    server/rta/communication/TCPRTACommunication.h
    server/rta/communication/TCPRTACommunication.cpp
    server/rta/communication/InfinibandRTACommunication.h
    server/rta/communication/InfinibandRTACommunication.cpp
    server/rta/Controller.cpp
    server/rta/Controller.h
    server/rta/dim_column_finder.h
    server/rta/dimension_record.cpp
    server/rta/dimension_record.h
    server/rta/dimension_schema.cpp
    server/rta/dimension_schema.h
    server/rta/utils.h
    server/query/AbstractQueryServerObject.cpp
    server/query/AbstractQueryServerObject.h
    server/query/Query1ServerObject.cpp
    server/query/Query1ServerObject.h
    server/query/Query2ServerObject.cpp
    server/query/Query2ServerObject.h
    server/query/Query3ServerObject.cpp
    server/query/Query3ServerObject.h
    server/query/Query4ServerObject.cpp
    server/query/Query4ServerObject.h
    server/query/Query5ServerObject.cpp
    server/query/Query5ServerObject.h
    server/query/Query6ServerObject.cpp
    server/query/Query6ServerObject.h
    server/query/Query7ServerObject.cpp
    server/query/Query7ServerObject.h
    server/sqlite/sqlite3.h
    server/sqlite/sqlite3.c
)

set(SEP_CLIENT_SRC
    sep-client/main.cpp
    sep-client/SEPClient.h
    sep-client/SEPClient.cpp
)

set(RTA_CLIENT_SRC
    rta-client/main.cpp
    rta-client/RTAClient.hpp
    rta-client/RTAClient.cpp
)

add_executable(aim_server ${SERVER_SRC} ${COMMON_SRC})
target_include_directories(aim_server PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(aim_server PUBLIC telldb)

add_executable(sep_client ${SEP_CLIENT_SRC} ${COMMON_SRC})
target_include_directories(sep_client PUBLIC ${Crossbow_INCLUDE_DIRS})

add_executable(rta_client ${RTA_CLIENT_SRC} ${COMMON_SRC})
target_include_directories(rta_client PUBLIC ${Crossbow_INCLUDE_DIRS})
