#include "AbstractSEPCommunication.h"

AbstractSEPCommunication::AbstractSEPCommunication(uint32_t port):
    _port(port),
    _queues(SEP_SERVER_QUEUES_SIZE)
{}
