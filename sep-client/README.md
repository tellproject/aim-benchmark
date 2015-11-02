I. Welcome
==========
That is the code for the sep-client. It consists of:
# code implementing the communication with server side
# code generating events

2. Building
===========
In order to build the project simply navigate to sep-client/:

$ mkdir build
$ cd build
$ cmake ..
$ -DCMAKE_BUILD_TYPE=Release -D_CMAKE_RECORDS_PER_BUCKET="" -D_CMAKE_SERVER_NUM="" -D_CMAKE_SEP_THREAD_NUM="" -D_CMAKE_RTA_THREAD_NUM="" -D_CMAKE_SUBSCR_NUM=""
$ make -j

e.g. -DCMAKE_BUILD_TYPE=release -D_CMAKE_RECORDS_PER_BUCKET=3072 -D_CMAKE_SERVER_NUM=1 -D_CMAKE_SEP_THREAD_NUM=1 -D_CMAKE_RTA_THREAD_NUM=2 -D_CMAKE_SUBSCR_NUM=1000000

We need some compile time constants:

# _CMAKE_RECORDS_PER_BUCKET: number of records per bucket
# _CMAKE_SERVER_NUM: number of servers being used in the experiment
# _CMAKE_SEP_THREAD_NUM: number of sep update threads
# _CMAKE_RTA_THREAD_NUM: number of scan thread
# _CMAKE_SUBSCR_NUM: the number of subscribers

_CMAKE_RTA_THREAD_NUM should be a multiple of _CMAKE_SEP_THREAD_NUM

5. Running
==========
Run the executable file existing in sep-client/build. Type:

$ ./sep-client <experiment-duration> <protocol> <server-file> <number-of-threads> <frequency>

In the following, the parameters are explained in detail:
# experiment-duration:  states how long the experiment (benchmark) should last. The time must be given in seconds.
# protocol: states the protocol to use, either TCP or Inf (for Infiniband).
# number-of-threads: number of client threads that should send events in parallel
# frequency: events per second that should be sent by all threads together
# server-file: the path to a text file that lists all the servers that queries should be sent to. The file must be structured as follows:
host1 port1
host2 port2

Example:

$ ./sep-client 3600 TCP server-addresses.txt 10 1000

This starts an experiment which lasts for an hour (3600 secs), uses TCP as communication protocol, sends queries to the servers defined in server-addresses.txt and generates a total load of 1,000 events per second. These events are sent by 10 threads in parallel.

