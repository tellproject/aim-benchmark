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
