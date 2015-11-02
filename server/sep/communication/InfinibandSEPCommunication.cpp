#include "util/logger.h"

#include "InfinibandSEPCommunication.h"

InfinibandSEPCommunication::InfinibandSEPCommunication(uint32_t port):
    AbstractSEPCommunication(port),
    _is_running(true),
    _infiniband(_port),
    _main_thread([&](){run();})
{
    LOG_INFO("Started listening for SEP requests on port " << port)
}

InfinibandSEPCommunication::~InfinibandSEPCommunication()
{
    _infiniband.stop();
    _is_running = false;
    _main_thread.join();
}

void
InfinibandSEPCommunication::run()
{

    char default_reply [1];
    default_reply[0] = '0';

    auto process_request = [&] (uint32_t qp_num, uint32_t msg_id, const char* buf,
            size_t size)
    {
        if (_is_running) {
            // immediately send void response to client
            Event event = Event::deserialize(buf);
            while(!_queues[event.callerId() % _queues.size()].write(std::move(event))) {

            }
            _infiniband.send_message(qp_num, msg_id, default_reply,
                    sizeof(default_reply));
        }
    };

    // this process run until m_isRunning is set to false
    _infiniband.run([](){}, process_request);
}
