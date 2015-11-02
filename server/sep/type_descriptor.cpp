#include "type_descriptor.h"

TypeDescriptor::TypeDescriptor(DataType type):
    _type(type)
{
    switch (_type) {
        case DataType::INT:
            _size = sizeof(int);
            break;
        case DataType::UINT:
            _size = sizeof(uint);
            break;
        case DataType::ULONG:
            _size = sizeof(ulong);
            break;
        case DataType::DOUBLE:
            _size = sizeof(double);
            break;
    }
}
