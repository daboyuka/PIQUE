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
 * bitmap-encode.cpp
 *
 *  Created on: Jun 17, 2014
 *      Author: David A. Boyuka II
 */

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/region/bitmap/bitmap-encode.hpp"

BitmapRegionEncoder::BitmapRegionEncoder(BitmapRegionEncoderConfig conf, size_t total_elements) :
	RegionEncoder< BitmapRegionEncoding, BitmapRegionEncoderConfig >(conf, total_elements),
	encoding(boost::make_shared< BitmapRegionEncoding >())
{}

boost::shared_ptr< BitmapRegionEncoding > BitmapRegionEncoder::to_region_encoding() {
	return this->encoding;
}

void BitmapRegionEncoder::push_bits_impl(uint64_t count, bool bitval) {
	const int BITS_PER_BLOCK = BitmapRegionEncoding::BITS_PER_BLOCK;

	const block_t bitval_mask = bitval ? ~(block_t)0 : (block_t)0;

	BitmapRegionEncoding &bitmap = *this->encoding;
	const uint64_t touched_blocks = (bitmap.domain_size + BITS_PER_BLOCK - 1) / BITS_PER_BLOCK;

	// First, correct the final block by filling its tail end with 0s/1s as appropriate
	const int touched_bits_in_last_block = bitmap.domain_size % BITS_PER_BLOCK;
	if (touched_bits_in_last_block) {
		const block_t touched_bits_mask = ((block_t)1 << touched_bits_in_last_block) - 1;

		block_t &last_block = bitmap.bits[touched_blocks - 1];
		last_block = (last_block & touched_bits_mask) | (bitval_mask & ~touched_bits_mask);
	}

	// Then, fill any blocks to be newly allocated with all 0s/1s as appropriate
	const uint64_t needed_blocks = (bitmap.domain_size + count + BITS_PER_BLOCK - 1) / BITS_PER_BLOCK;
	bitmap.bits.resize(needed_blocks, bitval_mask);

	bitmap.domain_size += count;
}
