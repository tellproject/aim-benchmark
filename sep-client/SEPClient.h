#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include "sep-client/communication/AbstractSEPClientCommunication.h"
#include "util/system-constants.h"

class SEPClient
{
public:
    /**
     * @brief SEPClient
     * creates a new SEPClient with the given number of threads, waiting time.
     * @param number_of_threads
     * @param wait_time
     * @param server_addresses_file
     * @param communication_type
     */
    SEPClient(uint8_t threads_num,
              ulong wait_time,
              std::string server_addresses_file,
              NetworkProtocol protocol);

    ~SEPClient();

    /**
     * @brief stop
     * sends the stop signal to all client threads, which means
     * they stop sending new queries and finish the query they are currently
     * working on.
     */
    void stop();

    /**
     * @brief waitFor
     * wait until all threads have ended.
     */
    void waitFor();

    /**
     * @brief getSentEvents
     * returns the total number of sent events by all threads.
     * @return
     */
    ulong getSentEvents();

private:
    bool _have_joined;
    uint8_t _threads_num;
    std::vector<std::unique_ptr<AbstractSEPClientCommunication> > _communication_modules;

};
