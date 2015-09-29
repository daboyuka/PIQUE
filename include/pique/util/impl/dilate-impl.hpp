/*
 * Copyright 2015 David A. Boyuka II
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
 */

/*
 * dilate-impl.hpp
 *
 * Bitwise integer dilation/undilation. Algorithms courtesy of
 * "Converting to and from Dilated Integers" by Raman and Wise
 * (http://www.cs.indiana.edu/~dswise/Arcee/castingDilated-comb.pdf).
 *
 *  Created on: Sep 16, 2015
 *      Author: David A. Boyuka II
 */
#ifndef SRC_UTIL_IMPL_DILATE_IMPL_HPP_
#define SRC_UTIL_IMPL_DILATE_IMPL_HPP_

//#include <boost/math/special_functions/pow.hpp>
#include <boost/integer/static_log2.hpp>

/*

// WIP - general multiplication-based implementation
template<typename word_t, int d>
struct DilaterHelper {
	static constexpr int w = std::numeric_limits<word_t>::digits;
	static constexpr int s = w / d;

	template<int i>
	class DilaterRound {
		static constexpr int di = std::pow(d, i);
		static constexpr int bitcount = (di < s ? di : s);

		template<int j>
		static constexpr word_t z = z<j-1> | (z<0> >> (j * di * d));

		template<>
		static constexpr word_t z<0> = ((1 << bitcount) - 1) << (d * (s - 1) + 1 - bitcount);


	};
};
*/

template<typename word_t, int d>
word_t Dilater<word_t, d>::dilate(word_t word) { abort(); }

template<typename word_t, int d>
word_t Dilater<word_t, d>::undilate(word_t word) { abort(); }

// Partial specialization for d=2
template<typename word_t>
class Dilater<word_t, 2> {
public:
	static word_t dilate(word_t word);
	static word_t undilate(word_t word);
};

namespace detail {
	template<typename word_t> constexpr int S() { return std::numeric_limits<word_t>::digits/2; } // num dilateable bits
	template<typename word_t> constexpr int ROUNDS() { return boost::static_log2< S<word_t>() >::value; }

	template<typename word_t, int power> constexpr word_t EXP2P1() { return ((word_t)1<<((word_t)1<<power)) + (word_t)1; }

	template<typename word_t, int round>
	struct rounds {
		// DILATION

		// Useful <rounds> go from 1 to ROUNDS inclusive (0 is just a base case, though it is used by UNDILATE_MASK below)
		// MASK<0> is a set mask of the lower half bits of word_t
		// Each successive MASK chops all groups of set bits in half (keeping the lower halves), then
		// duplicates those groups up by twice the width of these now smaller groups
		// Examples: for 8-bit: MASK<0> = 00001111, MASK<1> = 00110011, MASK<2> = 01010101
		static constexpr word_t DILATE_MASK() { return rounds<word_t, round-1>::DILATE_MASK() / EXP2P1<word_t, ROUNDS<word_t>()-round>() * EXP2P1<word_t, ROUNDS<word_t>()-round+1>(); }
		static constexpr word_t dilate_round(word_t word) {
			return
				word = rounds<word_t, round-1>::dilate_round(word),
				(word | (word << (S<word_t>() >> round))) & rounds<word_t, round>::DILATE_MASK();
		}

		// UNDILATION

		// Useful <rounds> go from 1 to ROUNDS inclusive (0 is just a base case)
		// UNDILATE_MASK is just DILATE_MASK in reverse order, shifted by one down
		// Examples: UNDILATE_MASK<1> is DILATE_MASK<ROUNDS-1>, UNDILATE_MASK<ROUNDS> is DILATE_MASK<0>
		static constexpr word_t UNDILATE_MASK() { return rounds<word_t, ROUNDS<word_t>()-round>::DILATE_MASK(); }
		static constexpr word_t undilate_round(word_t word) {
			return
				word = rounds<word_t, round-1>::undilate_round(word),
				(word | (word >> ((word_t)1 << (round-1)))) & rounds<word_t, round>::UNDILATE_MASK();
		}
	};

	template<typename word_t>
	struct rounds<word_t, 0> {
		static constexpr word_t DILATE_MASK() { return ((word_t)1 << S<word_t>()) - 1; }
		static constexpr word_t dilate_round(word_t word) { return word; }
		static constexpr word_t undilate_round(word_t word) { return word; }
	};

	template<typename word_t> constexpr word_t dilate(word_t word) { return rounds<word_t, ROUNDS<word_t>()>::dilate_round(word); }
	template<typename word_t> constexpr word_t undilate(word_t word) { return rounds<word_t, ROUNDS<word_t>()>::undilate_round(word); }
} // namespace detail

template<typename word_t>
word_t Dilater<word_t, 2>::dilate(word_t word) {
	return detail::dilate<word_t>(word);
}

template<typename word_t>
word_t Dilater<word_t, 2>::undilate(word_t word){
	return detail::undilate<word_t>(word);
}

#endif /* SRC_UTIL_IMPL_DILATE_IMPL_HPP_ */
