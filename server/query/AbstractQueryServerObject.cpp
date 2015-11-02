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
