#include "AbstractQueryServerObject.h"

#include <iostream>

AbstractQueryServerObject::AbstractQueryServerObject(uint32_t query_num,
                                                     uint8_t scan_threads,
                                                     AbstractRTACommunication* communication_object,
                                                     std::vector<char> additional_args):
    _query_num(query_num),
    _scan_threads(scan_threads),
    _additional_args(std::move(additional_args)),
    _communication_object(communication_object),
    _num_of_ended_threads(0)
{}

AbstractQueryServerObject::~AbstractQueryServerObject() {}

uint32_t
AbstractQueryServerObject::getQueryNr()
{
  return _query_num;
}

uint8_t
AbstractQueryServerObject::getNrOfScanThreads()
{
  return _scan_threads;
}

char*
AbstractQueryServerObject::getAdditionalArgs()
{
    return _additional_args.data();
}

void
AbstractQueryServerObject::setAdditionalArgs(
        std::vector<char> additionalArguments)
{
    _additional_args = std::move(additionalArguments);
}

void
AbstractQueryServerObject::notifyForResults()
{
    if (_communication_object)
        _communication_object->notifyForResults(this);
}

void
AbstractQueryServerObject::end()
{
    auto ended_threads = (_num_of_ended_threads.fetch_add(1)) + 1;
    assert(ended_threads <= _scan_threads);
    if (ended_threads == _scan_threads) {
        finishResult();
    }
}

void
AbstractQueryServerObject::finishResult()
{
    notifyForResults();
}
