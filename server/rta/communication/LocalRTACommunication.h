#pragma once

#include "rta/communication/AbstractRTACommunication.h"

/**
 * @brief The LocalRTACommunication class implements the RTA communication
 * through local memory for the standalone aim server.
 */
class LocalRTACommunication : public AbstractRTACommunication
{
public:

    LocalRTACommunication(uint8_t scan_threads,
                               uint8_t communication_threads,
                               const AIMSchema& aim_schema,
                               const DimensionSchema& dim_schema);

    ~LocalRTACommunication();

public:

    /**
     * @brief createAndEnqueueQuery
     * @param msg
     * @param finished pointer to a boolean (represented as uint_8)
     * that will be set to true when query is finished
     */
    void createAndEnqueueQuery(char*& msg, uint8_t *finished);

protected:

    void workerThreadMethod();

private:

    /**
     * @brief workerThreadMethod
     * the method that worker threads execute
     */
    std::vector<std::thread> _workerThreads;

};
