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

#include <boost/static_assert.hpp>
#include <boost/type_traits.hpp>
#include <boost/integer_traits.hpp>
#include <inttypes.h>
#include <limits>

namespace rtree {

/*!
 * \brief Minimum function for signed integers.
 * 
 * \tparam x	First value.
 * \tparam y	Second value.
 */
template <intmax_t x, intmax_t y>
struct Min {
	static const intmax_t value = x < y ? x : y;
};

/*!
 * \brief Maximum function for signed integers.
 * 
 * \tparam x	First value.
 * \tparam y	Second value.
 */
template <intmax_t x, intmax_t y>
struct Max {
	static const intmax_t value = x > y ? x : y;
};

/*!
 * \brief Recursive template meta-program for getting the logarithm base
 * 'base' of a value 'n'.
 * 
 * \tparam n					The value.
 * \tparam base					The base of the logarithm.
 * \tparam level				The recursion level of the template.
 */
template <
	uintmax_t n,
	uintmax_t base,
	uintmax_t level = 0
> struct Log {
	
	/*!
	 * \brief The result of the calculation.
	 * 
	 * Note how its definition triggers recursive template instantiation.
	 */
	static const uintmax_t value =
		Log<(n + base - 1) / base, base, level + 1>::value;
	
};

/*!
 * \brief Partial template specialization for the logarithm of 1.
 * 
 * When this template is instantiated, recursion bottoms-out and the
 * result of the calculation is just the recursion level.
 * 
 * \tparam base					The base of the logarithm.
 * \tparam level				The recursion level of the template, i.e.
 * 								the result of the calculation.
 */
template <uintmax_t base, uintmax_t level>
struct Log<1, base, level> {
	
	/*!
	 * \brief The result of the calculation. 
	 */
	static const uintmax_t value = level;
	
};

/*!
 * \brief Recursive template meta-program for the binary logarithm of n, rounded
 * up.
 * 
 * \tparam n			The number n.			
 * \tparam level		The recursion level.
 */
template <uintmax_t n, uintmax_t level = 0>
struct LogDual {
	
	/*!
	 * \brief The binary logarithm of n.
	 * 
	 * Note how the definition triggers recursive template instantiation. By
	 * dividing n by 2 at each recursion level, we have found the result when
	 * n reaches 1 (see partial specialization).
	 */
	static const uintmax_t value = LogDual<(n + 1) / 2, level + 1>::value;
	
};

/*!
 * \brief Partial template specialization for the binary logarithm of 1.
 * 
 * When this template is instantiated, recursion bottoms out and the recursion
 * level is the result.
 */
template <uintmax_t level>
struct LogDual<1u, level> {
	static const uintmax_t value = level;
};

/*!
 * \brief Recursive template meta-program for the exp'th power of a value n.
 * 
 * \tparam n					The base.
 * \tparam exp					The exponent.
 */
template <uintmax_t n, uintmax_t exp>
struct Pow {
	
	/*!
	 * \brief The result of the calculation.
	 * 
	 * Note how its definition triggers recursive template instantiation. 
	 */
	static const uintmax_t value = n * Pow<n, exp - 1>::value;
	
};

/*!
 * \brief Partial template specialization for the 0th power of any base.
 * 
 * \tparam n					The base.
 */
template <uintmax_t n>
struct Pow<n, 0> {
	
	/*!
	 * \brief The 0th power of any base is 1. 
	 */
	static const uintmax_t value = 1;
	
};

/*!
 * \brief Recursive template meta-program for the sum of the powers of a
 * base up to the exp'th power.
 * 
 * For example, PowSum<3, 4> would be 3^0 + 3^1 + 3^2 + 3^3 + 3^4 =
 * 1 + 3 + 9 + 27 + 81 = 121.
 * 
 * \tparam n					The base.
 * \tparam exp					The exponent.
 */
template <uintmax_t n, uintmax_t exp>
struct PowSum {
	
	/*!
	 * \brief The result of the calculation.
	 * 
	 * Note how its definition triggers recursive template instantiation. 
	 */
	static const uintmax_t value =
		Pow<n, exp>::value + PowSum<n, exp - 1>::value;
	
};

/*!
 * \brief Partial template specialization for the sum up to the 0th power of
 * any base.
 * 
 * \tparam n					The base.
 */
template <uintmax_t n>
struct PowSum<n, 0> {
	
	/*!
	 * \brief We define the sum of powers as 1 unless the base itself is 0.
	 * 
	 * Note that this is not mathematically correct. However, it makes
	 * the code that uses these calculations easier.
	 */
	static const uintmax_t value = n > 0;
	
};

/*!
 * Align a value to 
 */
template <uintmax_t value, uintmax_t alignment>
struct Align {
	
	static const uintmax_t result =
		(value + (alignment - 1)) & ~(alignment - 1);
	
private:
	
	BOOST_STATIC_ASSERT(result >= value);
	BOOST_STATIC_ASSERT(result <= value + (alignment - 1));
	BOOST_STATIC_ASSERT(!(result % alignment));
	
};

/*!
 * \brief Get the minimum non-null value for a given attribute type.
 */
template <typename T>
inline T min() throw() ALWAYS_INLINE;

/*!
 * \brief Get the maximum non-null value for a given attribute type.
 */
template <typename T>
inline T max() throw() ALWAYS_INLINE;

/*!
 * \brief Traits structure. Implements the functions above. Do not use directly.
 */
template <
	typename T,
	bool IS_INTEGRAL = boost::is_integral<T>::value,
	bool IS_FLOATING_POINT = boost::is_floating_point<T>::value,
	bool IS_SIGNED = boost::is_signed<T>::value
> struct Traits;

/*!
 * \brief Traits specialization for signed integer types.
 */
template <typename T>
struct Traits<T, true, false, true> {
	static inline T min() throw() ALWAYS_INLINE {
		return boost::integer_traits<T>::const_min + 1;
	}
	static inline T max() throw() ALWAYS_INLINE {
		return boost::integer_traits<T>::const_max;
	}
};

/*!
 * \brief Traits specialization for unsigned integer types.
 */
template <typename T>
struct Traits<T, true, false, false> {
	static inline T min() throw() ALWAYS_INLINE {
		return boost::integer_traits<T>::const_min;
	}
	static inline T max() throw() ALWAYS_INLINE {
		return boost::integer_traits<T>::const_max - 1;
	}
};

/*!
 * \brief Traits specialization for floating point types.
 */
template <typename T>
struct Traits<T, false, true, false> {
	static inline T min() throw() ALWAYS_INLINE {
		return std::numeric_limits<T>::min();
	}
	static inline T max() throw() ALWAYS_INLINE {
		return std::numeric_limits<T>::max();
	}
};

/*!
 * \brief Traits specialization for class types.
 */
template <typename T>
struct Traits<T, false, false, false> {
	static inline T min() throw() ALWAYS_INLINE {
		return T::getMin();
	}
	static inline T max() throw() ALWAYS_INLINE {
		return T::getMax();
	}
};

template <typename T>
inline T min() throw() {
	return Traits<T>::min();
}

template <typename T>
inline T max() throw() {
	return Traits<T>::max();
}

}
