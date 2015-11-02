#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>

#include "util/logger.h"
#include "rta-client/RTAClient.h"
#include "util/system-constants.h"

void
print_rta_client_args(int argc, const char* argv[])
{
    std::cout << "Experiment-duration: " << argv[1] << '\n'
              << "Protocol: " << argv[2] << '\n'
              << "Server-file: " << argv[3] << std::endl;
    for (uint32_t i = 4; i < argc; i+=2) {
        std::cout << "Workload-file: " << argv[i]
                     << " Threads: " << argv[i+1] << std::endl;
    }
}

/*
 * Wait until experiment is finished.
 */
void
wait(uint64_t exp_duration)
{
    std::chrono::high_resolution_clock::time_point current, notified;
    auto start = std::chrono::high_resolution_clock::now();
    while (true) {
        current = std::chrono::high_resolution_clock::now();
        if (std::chrono::duration_cast<std::chrono::duration<double>>
                (current - start).count() >= exp_duration) {
            return;
        }
        else {
            // notify user how much time of the experiment has passed
            long diff = std::chrono::duration_cast<std::chrono::duration
                    <double>>(current - notified).count();
            if (diff >= (exp_duration/100.0)) {
                notified = std::chrono::high_resolution_clock::now();
                diff = std::chrono::duration_cast<std::chrono::duration
                        <double>>(notified - start).count();
                std::cout << (int) (100.0 * diff / exp_duration)
                          << "% completed." << std::endl;
            }
            // sleep for a while
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }
}

/*
 * Stop the clients.
 */
void
stopRTAClients(std::vector<std::unique_ptr<RTAClient>> &clients)
{
    for (auto &client : clients) {
        client->stop();
    }
    return;
}

/*
 * Wait for clients to stop and output result.
 */
void result(std::vector<std::unique_ptr<RTAClient>> &clients)
{
    int count = 0;
    for (auto &client : clients) {
        while (!client->hasEnded());
        auto statistics = client->getStatistics();
        std::cout << "Summary for workload " << (count+1) << " [ms]:" << std::endl;
        for (uint8_t i=0; i<statistics.size(); ++i) {
            std::cout << "Q" << (i+1) << ": " << statistics[i][1]
                      << " (+/- " << statistics[i][2] << "), N = "
                      << statistics[i][0] << std::endl;
        }
        ++count;
    }
}

void printArgMsg()
{
    std::cout << "usage: rta-client <experiment-duration> <protocol>"
              << "<server-file> [<workload-file> <number-of-threads>]+"
              << "*experiment duration in seconds"
              << "*protocol, [Inf, TCP]"
              << "*server-file: each line contains 'host port'"
              << "*each <workload-file> is executed with "
              << "<number-of-threads> in parallel" << std::endl;
}

int main(int argc, const char* argv[])
{
    if (argc < 6 || (argc % 2 == 1)) {
        printArgMsg();
        return -1;
    }

#ifndef NDEBUG
    std::cout << "DEBUG\n" << std::endl;
#endif

    print_rta_client_args(argc, argv);
    uint64_t exp_duration(std::stol(argv[1]));
    std::string protocol_string(argv[2]);
    NetworkProtocol protocol = (protocol_string.compare("Inf")==0)?
                NetworkProtocol::Infiniband:
                NetworkProtocol::TCP;

    std::string server_file(argv[3]);

    // prepare client workloads
    std::vector<ClientWorkload> client_workloads;
    client_workloads.reserve((argc-4)/2);
    for (uint32_t i = 4; i < argc; i+=2) {
        client_workloads.emplace_back(argv[i], std::stoi(argv[i+1]));
    }

    // start clients
    auto clients = RTAClient::s_createRTAClients(client_workloads, server_file, protocol);

    std::cout << "Experiment started. Will wait for " << exp_duration
              << " seconds..." << std::endl;

    // wait until experiment is finished.
    wait(exp_duration);

    // stop the clients
    stopRTAClients(clients);

    // wait for clients to stop and output result
    result(clients);
}
