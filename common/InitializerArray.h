#pragma once

template<typename T, uint32_t SIZE>
struct InitializerArray {
    template<typename... Args>
    InitializerArray(Args&... args) {
        for (uint32_t i=0; i< SIZE; ++i) {
            new (data + i*sizeof(T)) T(args...);
        }
    }
    ~InitializerArray() {
        for (uint32_t i=0; i< SIZE; ++i) {
            (*this)[i].~T();
        }
    }
    T& operator[](uint32_t i) {
        return reinterpret_cast<T*>(data)[i];
    }
    constexpr uint32_t size() {
        return SIZE;
    }

    uint8_t data[SIZE*sizeof(T)];
};

