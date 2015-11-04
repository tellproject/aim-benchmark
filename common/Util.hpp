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
#include <random>
#include <cstdint>
#include <crossbow/singleton.hpp>
#include <crossbow/string.hpp>

#include "Protocol.hpp"

namespace std {

template<>
struct hash<std::pair<int16_t, int16_t>> {
    size_t operator() (std::pair<int16_t, int16_t> p) const {
        return size_t(size_t(p.first) << 16 | p.first);
    }
};

template<>
struct equal_to<std::pair<int16_t, int16_t>> {
    using type = std::pair<int16_t, int16_t>;
    bool operator() (type a, type b) const {
        return a.first == b.first && a.second == b.second;
    }
};

}

namespace aim {

// Stuff for generating random input
class Random_t {
public:
    using RandomDevice = std::mt19937;
private:
    friend struct crossbow::create_static<Random_t>;
    RandomDevice mRandomDevice;

    using DoubleDistr = std::uniform_real_distribution<double>;
    using LongDistr = std::uniform_int_distribution<ulong>;
    using IntDistr = std::uniform_int_distribution<uint>;
    DoubleDistr _double_distr;   //cost
    IntDistr _uint_distr;        //duration
    LongDistr _ulong_distr;      //caller-id, callee-id
    IntDistr _bool_distr;        //is_long_distance
    IntDistr _query_dist;
    IntDistr _q124_dist;        //q1.alpha, q2.alpha, q4.alpha
    IntDistr _q4_b_dist;
    IntDistr _q5_a_dist;
    IntDistr _q5_b_dist;
    IntDistr _q6_country_dist;
    IntDistr _q7_subscr_value_type_dist;

public: // Construction
    Random_t(size_t subscriberNum=10*1024*1024, uint8_t workloadSize = 1);

public:

    RandomDevice& randomDevice() { return mRandomDevice; }

    void randomEvent(Event &e);

    template<class I>
    I randomWithin(I lower, I upper) {
        std::uniform_int_distribution<I> dist(lower, upper);
        return dist(mRandomDevice);
    }

    uint8_t randomQuery();
    void randomQ1(Q1In &arg);
    void randomQ2(Q2In &arg);
    void randomQ4(Q4In &arg);
    void randomQ5(Q5In &arg);
    void randomQ6(Q6In &arg);
    void randomQ7(Q7In &arg);

};

int64_t now();

} // namespace aim

namespace crossbow {
extern template class singleton<aim::Random_t, create_static<aim::Random_t>, default_lifetime<aim::Random_t>>;
}

namespace aim {

using Random = crossbow::singleton<Random_t, crossbow::create_static<Random_t>, crossbow::default_lifetime<Random_t>>;

} // namespace aim

