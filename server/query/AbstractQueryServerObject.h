#pragma once

#include <atomic>
#include <cstdint>
#include <string>

//class forward declaration
class AbstractRTACommunication;

#include "column-map/ColumnMap.h"
#include "query/simd/queries.h"
#include "rta/communication/AbstractRTACommunication.h"
#include "sep/aim_column_finder.h"
#include "util/serialization.h"

/**
 * @brief The AbstractQueryServerObject class
 * represents an object that is used to collect results for a certain query.
 * The object is shared between several scan threads and a collector thread.
 * Scan threads add to the result by calling processBucket(.) resp.
 * processLastBucket(.) and end() if they have finished scanning their share.
 * The collector thread is responsible for creating the query object as well as
 * regularly poll for (partial) results (using popResult(.). In order to avoid
 * constant polling, the collector thread (that is of type
 * AbstractServerCommunication) is notified. Make sure that the notification
 * method of the collector thread only lists the object for polling and does
 * not poll directly because that would block the scan.
 */
class AbstractQueryServerObject
{
public:
    /**
     * @brief AbstractQueryServerObject
     * creates an abstract query server oject. Number of threads is an important
     * parameter for all query server objects.
     * @param queryNumber global (per server) identifier of query object
     * @param numberOfThreads number of scan threads that contribute to the result
     * @param communicationObject object that will be notified.
     * @param additionalArguments additional arguments that are used by the
     * collector thread
     */
    AbstractQueryServerObject(uint32_t queryNumber,
                              uint8_t numberOfThreads,
                              AbstractRTACommunication* communicationObject,
                              std::vector<char> additional_args);

    virtual ~AbstractQueryServerObject();

public:
    enum class Status: uint8_t {HAS_NEXT, DONE};

public:
    /**
     * @brief getAdditionalArguments
     * returns a pointer to the additional arguments of this object
     * @return
     */
    char* getAdditionalArgs();

    /**
     * @brief setAdditionalArguments
     * reset addtional arguments to a new value
     * @param additionalArguments
     */
    void setAdditionalArgs(std::vector<char> additionalArguments);

    /**
     * @brief processBucket
     * processes an entire bucket (by using SIMD instructions)
     */
    virtual void processBucket(uint thread_id, const BucketReference&) = 0;

    /**
     * @brief processLastBucket
     * processes the last bucket, which may be potentially empty
     */
    virtual void processLastBucket(uint thread_id, const BucketReference&) = 0;

    /**
     * @brief popResult
     * returns a partial result. The method should ONLY be called after
     * having been notified.
     * @return the result itself and a status whether this was the last result delivered.
     */
    virtual std::pair<std::vector<char>, Status> popResult() = 0;

    /**
     * @brief end
     * is used by a scan thread to indicate that it finished processing its share.
     */
    void end();

    /**
     * @brief getQueryNr is used by the RTA system to display the query nr
     *        when logging scan runtimes
     * @return m_queryNumber an integer identifier for the query object
     */
    virtual uint32_t getQueryNr();

    /**
     * @brief getNrOfScanThreads is called by the RTA system to display the nr
     *        of scan threads when logging scan runtimes
     * @return number of scan threads
     */
    virtual uint8_t getNrOfScanThreads();

protected:
    /**
     * @brief m_queryNumber
     * global (per server) identifier of query object
     */
    const uint32_t _query_num;

    /**
     * @brief m_numberOfScanThreads
     * number of threads that contribute to the result of this query object
     */
    const uint8_t _scan_threads;

    /**
     * @brief m_additionalArguments
     * additional arguments that can be given to the query object and are
     * used in the callback.
     */
    std::vector<char> _additional_args;

    /**
     * @brief finishResult
     * this function is called during the last call to end(.) before the
     * notification of the collector thread
     */
    virtual void finishResult();

protected:
    /**
     * @brief notifyForResults
     * notifies the communication object that a (partial) result is ready.
     */
    void notifyForResults();

private:
    /**
     * @brief m_communicationObject
     * this reference is used for notifying that a (partial) result is ready.
     */
    AbstractRTACommunication* _communication_object;
    std::atomic<uint8_t> _num_of_ended_threads;
};
