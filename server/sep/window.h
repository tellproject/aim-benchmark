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

#include "sep/utils.h"

/*
 * Describes the window of an AM attribute. A window has a type, a duration,
 * a length and information being used for its proper initialization.
 */
class Window
{
public:
    Window(WindowType, WindowLength, Timestamp init_info=FIRST_MONDAY);

public:
    Timestamp initInfo() const { return _init_info; }
    Timestamp duration() const { return _duration; }
    WindowLength length() const { return _length; }
    WindowType type() const { return _type; }

private:
    WindowType _type;           //window type (TUMB, STEP, CONT)
    Timestamp _duration;        //size of window (in msecs)
    WindowLength _length;   //DAY, WEEK
    Timestamp _init_info;       //when the window starts (in msecs)
};
