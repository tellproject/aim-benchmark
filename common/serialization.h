#pragma once
#include <type_traits>
#include <string>
#include <cstring>

namespace serialization {
struct Serializer {

    template<typename T, typename CHAR>
    typename std::enable_if<std::is_literal_type<T>::value, void>::type serialize(const T& t, CHAR* & destination) {
        static_assert(sizeof(CHAR) == 1, "your char has a strange size");
        *reinterpret_cast<T*>(destination) = t;
        destination += sizeof(T);
    }
    template<typename CHAR>
    void serialize(const std::string& t, CHAR* & destination) {
        static_assert(sizeof(CHAR) == 1, "your char has a strange size");
        *reinterpret_cast<std::string::size_type*>(destination) = t.size();
        destination += sizeof(std::string::size_type);
        std::memcpy(destination, t.c_str(), t.size());
        destination += t.size();
    }
    template<typename T, typename CHAR>
    typename std::enable_if<std::is_literal_type<T>::value, void>::type deserialize(CHAR* & source, T& destination) {
        static_assert(sizeof(CHAR) == 1, "your char has a strange size");
        destination = *reinterpret_cast<const T*>(source);
        source += sizeof(T);
    }

    template<typename CHAR>
    void deserialize(CHAR* & source, std::string& destination) {
        static_assert(sizeof(CHAR) == 1, "your char has a strange size");
        auto size = *reinterpret_cast<const std::string::size_type*>(source);
        source += sizeof(std::string::size_type);
        //TODO: copy
        destination = std::string(source, size);
        source += size;
    }
};
}

template<typename T, typename CHAR>
void serialize(const T& t, CHAR* & destination) {
    static_assert(sizeof(CHAR) == 1, "your char has a strange size");
    serialization::Serializer serializer;
    serializer.serialize(t, destination);
}


template<typename T, typename CHAR>
void deserialize(CHAR* & source, T& destination) {
    static_assert(sizeof(CHAR) == 1, "your char has a strange size");
    serialization::Serializer serializer;
    serializer.deserialize(source, destination);
}
