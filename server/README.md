I. Overview
============
The system consists of:
# The Stream and Event Processing (SEP) Sub-System and
# The RealTime Analytics (RTA) Sub-System

2. The SEP Sub-System
=====================
This is the code that implements the SEP benchmark. Existing folders are:
# server/sep:  Analytics Matrix component and Campaign Index
# server/sep/communication: Module for communication with sep-client

3. The RTA System
=================
This is the code that implements the RTA benchmark. Existing folders are:
# server/column-map: ColumnMap storage engine
# server/query: the queries living in the system
# server/rta: code responsible for handling updates and scans
# server/rta/communication: Module for communication with rta-client

4. Building
===========

4.1 Prerequisites
-----------------
- A 64-bit linux
- C++11 features were used for this project, consequently a very recent compiler is needed to (gcc 4.7 or newer) implementing these features is mandatory
- MySQL: sudo apt-get install mysql-server (we use username = 'root' and no password). For different username and password adjust accordingly in server/CMakeLists.txt
- create database 'meta_db' and load campaigns and analytics matrix attributes (AIMSchemaCampaignGenerator/300_campaigns_no_avg_3KB-records.sql) into the database
- sudo apt-get install build-essential libcppunit-dev libboost1.49-all-dev libibverbs-dev numactl libmysql++-dev libssl-dev cmake

4.2 Builds
----------
We use Cmake, the cross-platform, open-source build system for building the project. Cmake will first check whether there are missing dependencies. If not, a Makefile will be produced.

In order to build the project simply navigate to server/:

$ mkdir build
$ cmake .. -DCMAKE_BUILD_TYPE=Release -D_CMAKE_RECORDS_PER_BUCKET="" -D_CMAKE_SERVER_NUM="" -D_CMAKE_SEP_THREAD_NUM="" -D_CMAKE_RTA_THREAD_NUM="" -D_CMAKE_SUBSCR_NUM=""
$ make -j

e.g. -DCMAKE_BUILD_TYPE=release -D_CMAKE_RECORDS_PER_BUCKET=3072 -D_CMAKE_SERVER_NUM=1 -D_CMAKE_SEP_THREAD_NUM=1 -D_CMAKE_RTA_THREAD_NUM=2 -D_CMAKE_SUBSCR_NUM=1000000

We need some compile time constants:
# _CMAKE_RECORDS_PER_BUCKET: number of records per bucket
# _CMAKE_SERVER_NUM        : number of servers being used in the experiment
# _CMAKE_SEP_THREAD_NUM    : number of sep update threads
# _CMAKE_RTA_THREAD_NUM    : number of scan thread
# _CMAKE_SUBSCR_NUM        : the number of subscribers

note that _CMAKE_RTA_THREAD_NUM must be a multiple of _CMAKE_SEP_THREAD_NUM

5. Running
==========
A number of prints will appear in the terminal when the program starts running. These prints inform the user regarding the setup of the system. For instance: Number of AM attrs, Record size, Number of active campaigns. After that, population takes place. When population is completed a bunch of statistical information is displayed periodically.

Run the executable file existing in server/build. Type:

$ ./aim <Server Id> <SEP protocol> <RTA protocol>

# Server Id   : the id of the server (e.g. 0)
# SEP protocol: communication type with sep-client (Inf, TCP)
# RTA protocol: communication type with rta-client (Inf, TCP)

