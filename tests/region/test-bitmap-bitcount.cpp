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
 * test-bitmap-bitcount.cpp
 *
 *  Created on: Jan 26, 2015
 *      Author: David A. Boyuka II
 */

#include <cstdlib>
#include <cstdint>
#include <cassert>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/region/bitmap/bitmap.hpp"

static const uint64_t BIT_RANDOM_BITMAP_SIZE = (1ULL<<12) + 43;
static const double BIG_RANDOM_BITMAP_PROB = 0.1;

uint64_t make_random_bitmap(uint64_t nbits, double prob, std::vector< uint64_t > &bitvec) {
	uint64_t count = 0;
	boost::dynamic_bitset< uint64_t > bits(nbits, 0);

	for (uint64_t i = 0; i < nbits; i++) {
		const bool set = ((double)rand() / RAND_MAX) < prob;
		count += (set ? 1 : 0);
		bits[i] = set;
	}

	bitvec.resize(bits.num_blocks());
	boost::to_block_range(bits, bitvec.begin());
	return count;
}

int main(int argc, char **argv) {
	//                                     32 bits set        32 bits set        32 bits set         16 bits set
	std::vector< uint64_t > small_bitmap { 0x123456789ABCDEF, 0xFEDCBA987654321, 0x5A5A5A5A5A5A5A5A, 0x8421842184218421 };

	BitmapRegionEncoding small_aligned_bitmap(256, small_bitmap);
	assert(small_aligned_bitmap.get_element_count() == 32 + 32 + 32 + 16);

	BitmapRegionEncoding small_unaligned_bitmap(256 - 11, small_bitmap);
	assert(small_unaligned_bitmap.get_element_count() == 32 + 32 + 32 + 13); // 13 because the highest 3 set bits aren't counted due to the lower nbits

	std::vector< uint64_t > big_random;
	const uint64_t expected_big_random_count = make_random_bitmap(BIT_RANDOM_BITMAP_SIZE, BIG_RANDOM_BITMAP_PROB, big_random);

	// expected_big_random_count set
	BitmapRegionEncoding big_bitmap(BIT_RANDOM_BITMAP_SIZE, big_random);
	assert(big_bitmap.get_element_count() == expected_big_random_count);
}




