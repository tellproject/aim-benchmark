#include "window.h"

#include <cassert>

Window::Window(WindowType type, WindowLength length, Timestamp init_info):
     _type(type), _length(length), _init_info(init_info)
{
    if (length == WindowLength::DAY) {
        _duration = MSECS_PER_DAY;
    }
    else if (length == WindowLength::WEEK) {
        _duration = MSECS_PER_WEEK;
    }
    else {
        assert(false);
        _duration = MSECS_PER_WEEK;
    }
}
