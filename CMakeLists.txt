cmake_minimum_required(VERSION 3.0.2)
project(aim-benchmark)

# Set CMake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_package(Boost REQUIRED COMPONENTS system)
find_package(TellDB REQUIRED)
find_package(Crossbow REQUIRED)
find_package(Jemalloc REQUIRED)

# used for newer gcc versions that clash with Kudu
# --> make sure we compile old ABI
#add_definitions( -D_GLIBCXX_USE_CXX11_ABI=0 )

set(COMMON_SRC
    common/Protocol.cpp
    common/Util.cpp
    common/serialization.h
    common/dimension-tables-mapping.h
    common/dimension-tables-mapping.cpp
    common/dimension-tables.h
    common/dimension-tables.cpp
    common/dimension-tables-unique-values.h
    common/dimension-tables-unique-values.cpp
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mcx16")

find_package(Threads)

add_library(aim_common STATIC ${COMMON_SRC})
target_include_directories(aim_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(aim_common PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories(aim_common PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(aim_common PUBLIC ${Boost_LIBRARIES})
target_link_libraries(aim_common PUBLIC crossbow_logger)
target_link_libraries(aim_common PUBLIC telldb)

set(SERVER_COMMON_SRC
    server/sep/aim_schema.cpp
    server/sep/aim_schema.h
    server/sep/aim_schema_builder.cpp
    server/sep/aim_schema_builder.h
    server/sep/aim_schema_entry.cpp
    server/sep/aim_schema_entry.h
    server/sep/schema_and_index_builder.cpp
    server/sep/schema_and_index_builder.h
    server/sep/utils.cpp
    server/sep/utils.h
    server/sep/value.cpp
    server/sep/value.h
    server/sep/window.cpp
    server/sep/window.h
    server/rta/dimension_record.cpp
    server/rta/dimension_record.h
    server/rta/dimension_schema.cpp
    server/rta/dimension_schema.h
    server/rta/utils.h
    server/sqlite/sqlite3.h
    server/sqlite/sqlite3.c
)

set(SERVER_SRC
    ${SERVER_COMMON_SRC}
    server/main.cpp
    server/Connection.cpp
    server/Connection.hpp
    server/AimContext.hpp
    server/CreateSchema.cpp
    server/CreateSchema.hpp
    server/Populate.cpp
    server/Populate.hpp
    server/Q1Transaction.cpp
    server/Q2Transaction.cpp
    server/Q3Transaction.cpp
    server/Q4Transaction.cpp
    server/Q5Transaction.cpp
    server/Q6Transaction.cpp
    server/Q7Transaction.cpp
    server/ProcessEvent.cpp
)

set(SEP_CLIENT_SRC
    sep-client/main.cpp
    sep-client/SEPClient.hpp
    sep-client/SEPClient.cpp
)

set(RTA_CLIENT_SRC
    rta-client/main.cpp
    rta-client/RTAClient.hpp
    rta-client/RTAClient.cpp
)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/server/meta_db.db ${CMAKE_CURRENT_BINARY_DIR}/meta_db.db COPYONLY)

add_executable(aim_server ${SERVER_SRC})
target_link_libraries(aim_server PRIVATE aim_common telldb dl)
target_link_libraries(aim_server PUBLIC crossbow_allocator)
target_include_directories(aim_server PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(aim_server PRIVATE ${Jemalloc_LIBRARIES})

add_executable(sep_client ${SEP_CLIENT_SRC})
target_link_libraries(sep_client PRIVATE aim_common ${CMAKE_THREAD_LIBS_INIT})
target_include_directories(sep_client PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(sep_client PRIVATE ${Jemalloc_LIBRARIES})

add_executable(rta_client ${RTA_CLIENT_SRC})
target_link_libraries(rta_client PRIVATE aim_common ${CMAKE_THREAD_LIBS_INIT})
target_include_directories(rta_client PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(rta_client PRIVATE ${Jemalloc_LIBRARIES})

set(USE_KUDU OFF CACHE BOOL "Build AIM for Kudu")
if(${USE_KUDU})
    set(kuduClient_DIR "/mnt/local/tell/kudu_install/share/kuduClient/cmake")
    find_package(kuduClient REQUIRED)
    find_package(Threads)
    message(STATUS "Kudu header found at ${KUDU_CLIENT_INCLUDE_DIR}")

    set(KUDU_SERVER_SRC
        ${SERVER_COMMON_SRC}
        server/kudu.cpp
        server/CreateSchemaKudu.cpp
        server/PopulateKudu.cpp
        server/TransactionsKudu.cpp)

    add_executable(aim_kudu ${KUDU_SERVER_SRC})
    target_include_directories(aim_kudu PRIVATE ${KUDU_CLIENT_INCLUDE_DIR})
    target_compile_definitions(aim_kudu PRIVATE AIM_KUDU)
    target_link_libraries(aim_kudu PRIVATE kudu_client)
    target_link_libraries(aim_kudu PRIVATE aim_common telldb dl)
    target_link_libraries(aim_kudu PUBLIC crossbow_allocator)
    target_link_libraries(aim_kudu PRIVATE ${CMAKE_THREAD_LIBS_INIT})
    target_include_directories(aim_kudu PRIVATE ${Jemalloc_INCLUDE_DIRS})
    target_link_libraries(aim_kudu PRIVATE ${Jemalloc_LIBRARIES})
endif()
