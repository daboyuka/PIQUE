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
 * bitmap.cpp
 *
 * Note: count_bits implementation borrowed from here: http://dalkescientific.com/writings/diary/popcnt.c
 *
 *  Created on: Jun 17, 2014
 *      Author: David A. Boyuka II
 */

#include <iostream>

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/region/bitmap/bitmap.hpp"

constexpr RegionEncoding::Type BitmapRegionEncoding::TYPE;

static inline int popcount_fbsd2(const uint32_t *buf, int n) {
	int cnt = 0;
	do {
		unsigned v = *buf++;
		v -= ((v >> 1) & 0x55555555);
		v = (v & 0x33333333) + ((v >> 2) & 0x33333333);
		v = (v + (v >> 4)) & 0x0F0F0F0F;
		v = (v * 0x01010101) >> 24;
		cnt += v;
	} while (--n);
	return cnt;
}

/* Process 3 words at a time */
static inline int merging2(const uint32_t *data) {
	unsigned count1, count2, half1, half2;
	count1 = data[0];
	count2 = data[1];
	half1 = data[2] & 0x55555555;
	half2 = (data[2] >> 1) & 0x55555555;
	count1 = count1 - ((count1 >> 1) & 0x55555555);
	count2 = count2 - ((count2 >> 1) & 0x55555555);
	count1 += half1;
	count2 += half2;
	count1 = (count1 & 0x33333333) + ((count1 >> 2) & 0x33333333);
	count2 = (count2 & 0x33333333) + ((count2 >> 2) & 0x33333333);
	count1 += count2;
	count1 = (count1 & 0x0F0F0F0F) + ((count1 >> 4) & 0x0F0F0F0F);
	count1 = count1 + (count1 >> 8);
	count1 = count1 + (count1 >> 16);
	return count1 & 0x000000FF;
}

static inline int merging3(const uint32_t *data) {
	unsigned count1, count2, half1, half2, acc = 0;
	int i;
	for (i = 0; i < 24; i += 3) {
		count1 = data[i];
		count2 = data[i + 1];
		//w = data[i+2];
		half1 = data[i + 2] & 0x55555555;
		half2 = (data[i + 2] >> 1) & 0x55555555;
		count1 = count1 - ((count1 >> 1) & 0x55555555);
		count2 = count2 - ((count2 >> 1) & 0x55555555);
		count1 += half1;
		count2 += half2;
		count1 = (count1 & 0x33333333) + ((count1 >> 2) & 0x33333333);
		count1 += (count2 & 0x33333333) + ((count2 >> 2) & 0x33333333);
		acc += (count1 & 0x0F0F0F0F) + ((count1 >> 4) & 0x0F0F0F0F);
	}
	acc = (acc & 0x00FF00FF) + ((acc >> 8) & 0x00FF00FF);
	acc = acc + (acc >> 16);
	return acc & 0x00000FFFF;
}

/* count 24 words at a time, then 3 at a time, then 1 at a time */
static inline int popcount_24words(const uint32_t *buf, uint64_t n) {
	uint64_t cnt = 0, i;
	for (i = 0; i < n - n % 24; i += 24)
		cnt += merging3(buf + i);
	for (; i < n - n % 3; i += 3)
		cnt += merging2(buf + i);
	if (i != n)
		cnt += popcount_fbsd2(buf + i, n - i);
	return cnt;
}

void BitmapRegionEncoding::dump() const {
	uint64_t bitpos = 0;
	char blockstr[BITS_PER_BLOCK + 1];
	for (auto blockit = this->bits.cbegin(); blockit != this->bits.cend(); ++blockit) {
		const block_t block = *blockit;

		int bitpos_in_block = 0;
		for (block_t mask = 1; bitpos_in_block < BITS_PER_BLOCK && bitpos < domain_size; bitpos++, bitpos_in_block++, mask <<= 1)
			blockstr[bitpos_in_block] = (block & mask) ? '1' : '0';

		if (bitpos_in_block == 0)
			break;

		blockstr[bitpos_in_block] = '\0'; // NULL-terminate
		std::cout << blockstr;
		if (blockit != this->bits.cbegin())
			std::cout << " ";
	}
	std::cout << std::endl;
}

uint64_t BitmapRegionEncoding::get_element_count() const {
	const uint64_t full_word_count = this->domain_size / BitmapRegionEncoding::BITS_PER_BLOCK;
	const int remaining_bits = this->domain_size - full_word_count * BitmapRegionEncoding::BITS_PER_BLOCK;

	uint64_t count = 0;

	// First, count the bits in all the full blocks, fast
	// Note: popcount_24words takes uint32_ts, so double the number of full 64-bit words to pass in
	count += popcount_24words(reinterpret_cast< const uint32_t * >(&this->bits.front()), full_word_count * 2);

	// Then, add the set bits in the last partial block
	if (remaining_bits) {
		uint64_t last_word = this->bits[full_word_count];
		for (int bitpos = 0; bitpos < remaining_bits; ++bitpos) {
			count += last_word & 1;
			last_word >>= 1;
		}
	}

	return count;
}

void BitmapRegionEncoding::convert_to_rids(std::vector<uint32_t>& out, bool sorted, bool preserve_self) {
	out.clear();
	uint64_t bitpos = 0;
	for (auto blockit = this->bits.cbegin(); blockit != this->bits.cend(); ++blockit) {
		const block_t block = *blockit;

		for (int bitpos_in_block = 0; bitpos_in_block < BITS_PER_BLOCK && bitpos < domain_size; ++bitpos, ++bitpos_in_block)
			if (block & (1ULL << bitpos_in_block))
				out.push_back(static_cast<uint32_t>(bitpos));
	}
}

void BitmapRegionEncoding::to_bitset(boost::dynamic_bitset<block_t> &out) {
	out.clear();
	out.append(bits.begin(), bits.end());
	out.resize(domain_size);
}

void BitmapRegionEncoding::zero() {
	bits.clear();
	bits.resize((domain_size + BITS_PER_BLOCK - 1) / BITS_PER_BLOCK, 0);
}

void BitmapRegionEncoding::fill() {
	bits.clear();
	bits.resize((domain_size + BITS_PER_BLOCK - 1) / BITS_PER_BLOCK, ~(block_t)0);
}

bool BitmapRegionEncoding::operator==(const RegionEncoding& other_base) const {
	if (typeid(other_base) != typeid(BitmapRegionEncoding))
		return false;
	const BitmapRegionEncoding &other = dynamic_cast<const BitmapRegionEncoding&>(other_base);

	if (this->domain_size != other.domain_size)
		return false;

	const uint64_t num_full_blocks = this->domain_size / BITS_PER_BLOCK;
	uint64_t blockpos = 0;
	for (auto thisit = this->bits.cbegin(), otherit = other.bits.cbegin(); blockpos < num_full_blocks; ++blockpos, ++thisit, ++otherit)
		if (*thisit != *otherit)
			return false;

	const int bits_in_partial_tail_word = this->domain_size % BITS_PER_BLOCK;
	if (bits_in_partial_tail_word != 0) {
		const block_t mask = ((block_t)1 << bits_in_partial_tail_word) - 1;
		if ((this->bits[num_full_blocks] & mask) != (other.bits[num_full_blocks] & mask))
			return false;
	}

	return true;
}





