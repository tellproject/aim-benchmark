I. Overview
===========
That is the code for the rta-client. It consists of:
# code implementing the communication with server side
# queries

2. Building
===========
In order to build the project simply navigate to rta-client/ folder and type:

$ mkdir build
$ cd build
$ cmake .. -DCMAKE_BUILD_TYPE=Release -D_CMAKE_RECORDS_PER_BUCKET="" -D_CMAKE_SERVER_NUM="" -D_CMAKE_SEP_THREAD_NUM="" -D_CMAKE_RTA_THREAD_NUM="" -D_CMAKE_SUBSCR_NUM=""
$ make -j

e.g. -DCMAKE_BUILD_TYPE=release -D_CMAKE_RECORDS_PER_BUCKET="" -D_CMAKE_SERVER_NUM=1 -D_CMAKE_SEP_THREAD_NUM=1 -D_CMAKE_RTA_THREAD_NUM=2 -D_CMAKE_SUBSCR_NUM=1000000

We need some compile time constants:
# _CMAKE_RECORDS_PER_BUCKET: number of records per bucket
# _CMAKE_SERVER_NUM: number of servers being used in the experiment
# _CMAKE_SEP_THREAD_NUM: number of sep update threads
# _CMAKE_RTA_THREAD_NUM: number of scan thread
# _CMAKE_SUBSCR_NUM: the number of subscribers

_CMAKE_RTA_THREAD_NUM should be a multiple of _CMAKE_SEP_THREAD_NUM

3. Running
==========

$ ./rta-client <experiment-duration> <protocol> <server-file> [<workload-file> <number-of-threads>]+

In the following, the parameters are explained in detail:

# experiment-duration: states how long the experiment (benchmark) should last. The time must be given in seconds.
# protocol: states the protocol to use, either TCP or Inf (for Infiniband).
# [<workload-file> <number-of-threads>]+:
 * workload-file: path to a file that describes the workload
 * number-of-threads: number of threads that execute this workload
# server-file: the path to a text file that lists all the servers that queries should be sent to. The file must be structured as follows:
host1 port1
host2 port2
...

The workload is executed in a closed-system manner (i.e. a thread asks a query, then waits until it gets a result and only then asks the next query).

Workload files have one line per query that should be executed. Query themselves are represented by 1-7 and the parameters for running these queries are chosen at random. Each thread starts at a random line of the workload and restarts at the beginning when it reaches the end. It loops as long as the experiment lasts. Response times are aggregated per query per workload. We currently report on mean, standard deviation and sample size (N) of response time. This response time includes sending the query to the servers, waiting for resuls and collecting and aggregating them. In order to find out how much of this time is spent at the server side, there should be separate measurements in the server code.

Execution example:
$ ./rta-client 3600 TCP server-addresses.txt aggregation-only.txt 2 mixed.txt 3	select-only.txt 1

This starts an experiment which lasts for an hour (3600 secs), uses TCP as communication protocol, sends queries to the servers defined in server-addresses.txt and executes three different workloads with different number of threads (2 threads on aggregation-only, 3 threads on mixed and 1 thread on select-only).

