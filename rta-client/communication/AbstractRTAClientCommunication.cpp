#include "AbstractRTAClientCommunication.h"

#include <thread>

#include "query/Query1ClientObject.h"
#include "query/Query2ClientObject.h"
#include "query/Query3ClientObject.h"
#include "query/Query4ClientObject.h"
#include "query/Query5ClientObject.h"
#include "query/Query6ClientObject.h"
#include "query/Query7ClientObject.h"

AbstractRTAClientCommunication::AbstractRTAClientCommunication(
        const std::vector<ServerAddress>& serverAddresses
    ) :
    _serverAddresses(serverAddresses),
    _numberOfServers(serverAddresses.size())
{}

AbstractRTAClientCommunication::~AbstractRTAClientCommunication()
{}

std::vector<char>
AbstractRTAClientCommunication::processQueryMultiThreaded(
        AbstractQueryClientObject& queryObject)
{
    // start one thread per server
    std::vector<std::thread> currentThreads;
    currentThreads.reserve(_numberOfServers);
    for (uint i=0; i<_numberOfServers; ++i) {
        currentThreads.emplace_back([&, i](){processQuery(i, queryObject);});
    }

    // wait for threads to join
    for (auto &thread: currentThreads) {
        thread.join();
    }

    // return result
    return queryObject.getResult();
}

std::vector<char>
AbstractRTAClientCommunication::processQuery1(uint32_t alpha)
{
    // prepare send buffer
    *(reinterpret_cast<uint32_t*>(_sendBuf)) = 1;
    *(reinterpret_cast<uint32_t*>(&_sendBuf[sizeof(uint32_t)])) = alpha;
    _sendBufSize = 2 * sizeof(uint32_t);

    // prepare query1 client object
    Query1ClientObject queryObject;

    // process query multithreaded
    return processQueryMultiThreaded(queryObject);
}

std::vector<char>
AbstractRTAClientCommunication::processQuery2(uint32_t alpha)
{
    // prepare send buffer
    *(reinterpret_cast<uint32_t*>(_sendBuf)) = 2;
    *(reinterpret_cast<uint32_t*>(&_sendBuf[sizeof(uint32_t)])) = alpha;
    _sendBufSize = 2 * sizeof(uint32_t);

    // prepare query2 client object
    Query2ClientObject queryObject;

    // process query multithreaded
    return processQueryMultiThreaded(queryObject);
}

std::vector<char>
AbstractRTAClientCommunication::processQuery3()
{
    // prepare send buffer
    *(reinterpret_cast<uint32_t*>(_sendBuf)) = 3;
    _sendBufSize = sizeof(uint32_t);

    // prepare query3 client object
    Query3ClientObject queryObject;

    // process query multithreaded
    return processQueryMultiThreaded(queryObject);
}

std::vector<char>
AbstractRTAClientCommunication::processQuery4(uint32_t alpha, uint32_t beta)
{
    // prepare send buffer
    *reinterpret_cast<uint32_t*>(_sendBuf) = 4;
    *reinterpret_cast<uint32_t*>(&_sendBuf[sizeof(uint32_t)]) = alpha;
    *reinterpret_cast<uint32_t*>(&_sendBuf[sizeof(uint32_t) + sizeof(alpha)]) = beta;
    _sendBufSize = 3 * sizeof(uint32_t);

    // prepare query4 client object
    Query4ClientObject queryObject;

    // process query multithreaded
    return processQueryMultiThreaded(queryObject);
}

std::vector<char> AbstractRTAClientCommunication::processQuery5(uint16_t alpha, uint16_t beta)
{
    *reinterpret_cast<uint32_t*>(_sendBuf) = 5;
    *reinterpret_cast<uint16_t*>(&_sendBuf[sizeof(uint32_t)]) = alpha;
    *reinterpret_cast<uint16_t*>(&_sendBuf[sizeof(uint32_t) + sizeof(alpha)]) = beta;
    _sendBufSize = sizeof(uint32_t) + 2 * sizeof(uint16_t);

    // prepare query5 client object
    Query5ClientObject queryObject;

    // process query multithreaded
    return processQueryMultiThreaded(queryObject);
}

std::vector<char> AbstractRTAClientCommunication::processQuery6(uint16_t country_id)
{
    *reinterpret_cast<uint32_t*>(_sendBuf) = 6;
    *reinterpret_cast<uint16_t*>(&_sendBuf[sizeof(uint32_t)]) = country_id;
    _sendBufSize = sizeof(uint32_t) + sizeof(uint16_t);

    // prepare query6 client object
    Query6ClientObject queryObject;

    // process query multithreaded
    return processQueryMultiThreaded(queryObject);
}

std::vector<char> AbstractRTAClientCommunication::processQuery7(uint16_t window_length, uint16_t subscriber_value_type)
{
    *reinterpret_cast<uint32_t*>(_sendBuf) = 7;
    *reinterpret_cast<uint16_t*>(&_sendBuf[sizeof(uint32_t)]) = window_length;
    *reinterpret_cast<uint16_t*>(&_sendBuf[sizeof(uint32_t) + sizeof(window_length)]) = subscriber_value_type;
    _sendBufSize = sizeof(uint32_t) + 2 * sizeof(uint16_t);

    // prepare query7 client object
    Query7ClientObject queryObject;

    // process query multithreaded
    return processQueryMultiThreaded(queryObject);

}
