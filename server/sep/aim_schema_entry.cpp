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
#include "aim_schema_entry.h"

#include <cassert>

AIMSchemaEntry::AIMSchemaEntry(Value value, Window window, InitDefFPtr init_def,
                               InitFPtr init, UpdateFPtr update, MaintainFPtr
                               maintain, FilterType filter_type, FilterFPtr filter,
                               InitDefFPtr simple_init_def, InitFPtr simple_init,
                               UpdateFPtr simple_update, MaintainFPtr simple_maintain):

    _value(value), _window(window), _size(value.dataSize()),
    _init_def(init_def), _init(init), _update(update),
    _maintain(maintain), _filter_type(filter_type), _filter(filter),
    _simple_init_def(simple_init_def), _simple_init(simple_init),
    _simple_update(simple_update), _simple_maintain(simple_maintain)
{}
