#include "aim_schema_entry.h"

#include <cassert>

AIMSchemaEntry::AIMSchemaEntry(Value value, Window window, InitDefFPtr init_def,
                               InitFPtr init, UpdateFPtr update, MaintainFPtr
                               maintain, FilterType filter_type, FilterFPtr filter):

    _value(value), _window(window), _size(value.metricDataSize()),
    _init_def(init_def), _init(init), _update(update),
    _maintain(maintain), _filter_type(filter_type), _filter(filter)
{
    assert(value.aggrDataSize() == value.metricDataSize());
}
