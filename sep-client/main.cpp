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
#include <chrono>
#include <cstdint>
#include <thread>

#include "sep-client/SEPClient.h"
#include "util/logger.h"
#include "util/system-constants.h"

void print_sep_client_args(const char* argv[])
{
    std::cout << "Experiment-duration: " << argv[1] << '\n'
              << "Protocol: " << argv[2] << '\n'
              << "Server-file: " << argv[3] << '\n'
              << "Number of threads: " << argv[4] << '\n'
              << "SEP update rate: " << argv[5] << std::endl;
}

void printArgMsg()
{
    std::cout << "usage: sep-client <experiment-duration> <protocol> "
              << "<server-file> <number-of-threads> <frequency>" << std::endl;
    std::cout << "*experiment duration in seconds" << std::endl;
    std::cout << "*OPTIONS for protocol: TCP, Inf (Infiniband)" << std::endl;
    std::cout << "*server-file: each line contains 'host port'" << std::endl;
    std::cout << "frequency in events per second" << std::endl;
}

int main(int argc, const char* argv[])
{
    if (argc != 6 ) {
        printArgMsg();
        return -1;
    }

#ifndef NDEBUG
    std::cout << "DEBUG\n" << std::endl;
#endif

    print_sep_client_args(argv);
    uint64_t experiment_duration(std::stol(argv[1]));
    std::string protocol_string(argv[2]);
    NetworkProtocol protocol = (protocol_string.compare("Inf")==0)?
                NetworkProtocol::Infiniband:
                NetworkProtocol::TCP;

    std::string server_file(argv[3]);
    uint8_t number_of_threads(std::stol(argv[4]));
    // wait time in micro-seconds
    ulong wait_time = (1000000 * number_of_threads) / (std::stol(argv[5]));

    SEPClient client(number_of_threads, wait_time, server_file, protocol);

    std::cout << "Experiment started. Will wait for " << experiment_duration
              << " seconds..." << std::endl;

    // wait until experiment is finished
    std::chrono::high_resolution_clock::time_point current, notified;
    auto start = std::chrono::high_resolution_clock::now();
    while (true) {
        current = std::chrono::high_resolution_clock::now();
        if (std::chrono::duration_cast<std::chrono::duration<double>>
                (current - start).count() >= experiment_duration) {
            break;
        }
        else {
            // notify user how much time of the experiment has passed
            long diff = std::chrono::duration_cast<std::chrono::duration
                    <double>>(current - notified).count();
            if (diff >= (experiment_duration/100.0)) {
                notified = std::chrono::high_resolution_clock::now();
                diff = std::chrono::duration_cast<std::chrono::duration
                        <double>>(notified - start).count();
                std::cout << (int) (100.0 * diff / experiment_duration)
                          << "% completed." << std::endl;
            }
            // sleep for a while
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

    client.stop();
    client.waitFor();

    auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now() - start).count();
    ulong sent_events = client.getSentEvents();
    std::cout << "Sent " << sent_events << " events in "
              << (((double) diff) / 1000.0) << " seconds." << std::endl;
    std::cout << "This makes an effective send-rate of "
              << (1000.0 * ((double) sent_events)/((double) diff))
              << " events per second." << std::endl;
}
