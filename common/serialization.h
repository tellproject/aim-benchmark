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
