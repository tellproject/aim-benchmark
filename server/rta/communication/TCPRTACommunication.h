#pragma once

#include "query/AbstractQueryServerObject.h"
#include "rta/communication/AbstractRTACommunication.h"

/**
 * @brief The TCPRTACommunication class implements the RTA communication
 * over TCP/IP connections. The communication threads can be blocked by
 * heavy messages that come from the same query object.
 * TODO: optimize this once we consider several communication threads
 */
class TCPRTACommunication : public AbstractRTACommunication
{
public:
    TCPRTACommunication(uint32_t port_num,
                        uint8_t scan_threads,
                        uint8_t communication_threads,
                        const AIMSchema& aim_schema,
                        const DimensionSchema& dim_schema);

    ~TCPRTACommunication();

protected:
    void run();

private:
    /**
     * @brief m_mainThread
     * handle for the main thread. The main thread is created and started
     * automatically by the constructor of the class.
     */
    std::thread _main_thread;

private:
    /**
     * @brief workerThreadMethod
     * the method that worker threads execute
     */
    void _workerThreadMethod();
};
