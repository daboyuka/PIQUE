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
 * test-dilate.cpp
 *
 *  Created on: Sep 17, 2015
 *      Author: David A. Boyuka II
 */

#include <cstdlib>
#include <boost/random/mersenne_twister.hpp>

#include "pique/util/dilate.hpp"

template<typename word_t>
void test_rand() {
	boost::mt19937_64 rand; // default seed is fine

	static constexpr int N = 1000;
	for (int i = 0; i < N; ++i) {
		const word_t word = (word_t)(rand() >> (std::numeric_limits<uint64_t>::digits - std::numeric_limits<word_t>::digits/2)); // restrict the bits we use to the low half for valid dilation
		const word_t dilated_word = Dilater<word_t, 2>::dilate(word);
		const word_t word2 = Dilater<word_t, 2>::undilate(dilated_word);
		assert(word == word2);
	}
}

template<typename word_t>
void test_boundary_cases() {
	word_t dilated_word, undilated_word;

	const word_t zero_undilated = 0;
	const word_t zero_dilated = 0;
	dilated_word = Dilater<word_t, 2>::dilate(zero_undilated);
	assert(dilated_word == zero_dilated);
	undilated_word = Dilater<word_t, 2>::undilate(dilated_word);
	assert(undilated_word == zero_undilated);


	const word_t max_undilated = (word_t)~((word_t)0) >> (std::numeric_limits<word_t>::digits/2);
	word_t max_dilated = 0;
	for (int i = 0; i < std::numeric_limits<word_t>::digits; i += 2)
		max_dilated |= ((word_t)1 << i);

	dilated_word = Dilater<word_t, 2>::dilate(max_undilated);
	assert(dilated_word == max_dilated);
	undilated_word = Dilater<word_t, 2>::undilate(dilated_word);
	assert(undilated_word == max_undilated);
}

template<typename word_t>
void test_word_type() {
	test_rand<word_t>();
	test_boundary_cases<word_t>();
}

int main(int argc, char **argv) {
	test_word_type<uint8_t>();
	test_word_type<uint16_t>();
	test_word_type<uint32_t>();
	test_word_type<uint64_t>();
}

