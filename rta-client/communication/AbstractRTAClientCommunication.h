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
#pragma once

#include <cstdint>
#include <string>
#include <sys/types.h>

#include "query/AbstractQueryClientObject.h"
#include "util/system-constants.h"

class AbstractRTAClientCommunication
{
public:
    /**
     * @brief AbstractClientCommunication
     * creates a new abstract client communication object with
     * host and port of the servers, it should communicate to. Note that
     * communication object does not run in a separate thread. Instead,
     * calls to process a certain query block until the result is delivered.
     * However, while query processing, sending requests to the servers and
     * aggregating the results is done in parallel.
     * @param serverAddresses
     */
    AbstractRTAClientCommunication(const std::vector<ServerAddress>& serverAddresses);

    virtual ~AbstractRTAClientCommunication();

    /**
     * @brief processQuery1
     * sends a request for query 1 to the server and collects the result.
     * @param alpha parameter alpha of query 1
     * @return the serialized result
     */
    std::vector<char> processQuery1(uint32_t alpha);

    /**
     * @brief processQuery2
     * sends a request for query 2 to the server and collects the result.
     * @return the serialized result
     */
    std::vector<char> processQuery2(uint32_t alpha);

    /**
     * @brief processQuery3
     * sends a request for query 3 to the server and collects the result.
     * @return the serialized result
     */
    std::vector<char> processQuery3();

    /**
     * @brief processQuery4
     * sends a request for query 4 to the server and collects the result.
     * @param alpha, beta parameters alpha, beta of query 4
     * @return the serialized result
     */
    std::vector<char> processQuery4(uint32_t alpha, uint32_t beta);

    /**
     * @brief processQuery5
     * sends a request for query 5 to the server and collects the result.
     * @param alpha, beta parameters alpha, beta of query 5
     * @return the serialized result
     */
    std::vector<char> processQuery5(uint16_t alpha, uint16_t beta);

    /**
     * @brief processQuery6
     * sends a request for query 6 to the server and collects the result.
     * @param country_id parameter country_id of query 6
     * @return
     */
    std::vector<char> processQuery6(uint16_t country_id);

    /**
     * @brief processQuery7
     * sends a request for query 7 to the server and collects the result.
     * @param window_type, subscriber_value_type parameber window_type and
     * subscriber_value_typ of query 7
     * @return
     */
    std::vector<char> processQuery7(uint16_t window_type, uint16_t subscriber_value_type);
protected:
    /**
     * @brief m_numberOfServers
     * number of query servers
     */
    uint32_t _numberOfServers;

    /**
     * @brief m_serverAddresses
     * addresses of the different servers. Addresses consist of host and port.
     */
    std::vector<ServerAddress> _serverAddresses;

    /**
     * @brief m_sendBuf
     * we keep one single send buffer and send the request to several servers
     */
    char _sendBuf[RTA_REQUEST_BUFFER_SIZE];

    /**
     * @brief m_sendBufSize
     * we keep the size of the current request separately
     */
    size_t _sendBufSize;

    /**
     * @brief processQueryMultiThreaded
     * calls processQuery once for every server in a different thread
     * @param queryObject
     * @return the byte-length of the result buffer
     */
    std::vector<char> processQueryMultiThreaded(
            AbstractQueryClientObject & queryObject
        );

    /**
     * @brief processQuery
     * internal function that is called once for every thread. Once this
     * function is called, m_sendBuf and m_sendBufSize are already set to the
     * values that encode the query.
     * @param threadNumber
     * @param queryObject
     */
    virtual void processQuery(uint32_t threadNumber,
                              AbstractQueryClientObject & queryObject) = 0;

};
