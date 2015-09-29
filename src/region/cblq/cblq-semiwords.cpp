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
 * cblq-semiwords.cpp
 *
 *  Created on: Mar 27, 2014
 *      Author: David A. Boyuka II
 */

#include <vector>

#include "pique/region/cblq/cblq.hpp"
#include "pique/util/dilate.hpp"

// Class for handling the dense prefix/suffix structure (since CBLQSemiwords
// are too small for even a char at ndim == 1 or 2)
// TODO: Consider generalizing this to a sub-byte word stream
template<int ndim>
CBLQSemiwords<ndim>::iterator::iterator(CBLQSemiwords &dw, size_t pos) :
	dw(dw),
	pos(pos),
	block_pos(pos / SEMIWORDS_PER_BLOCK),
	intrablock_pos((pos % SEMIWORDS_PER_BLOCK) * CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD)
{
	assert(pos <= dw.num_semiwords);
}

template<int ndim>
typename CBLQSemiwords<ndim>::cblq_semiword_t
CBLQSemiwords<ndim>::iterator::top() const {
	return (dw.semiwords[block_pos] >> intrablock_pos) & SEMIWORD_MASK;
}

template<int ndim>
typename CBLQSemiwords<ndim>::cblq_word_t
CBLQSemiwords<ndim>::iterator::top_fullword() const {
	const cblq_semiword_t topword = top();
	const cblq_word_t dilated = Dilater<cblq_word_t, 2>::dilate(topword); // NOTE: dilating 0b11 == 0b0101
	const cblq_word_t unshifted = dilated << (dw.encoding_two_codes ? 1 : 0);
	return unshifted;
}

template<int ndim>
void
CBLQSemiwords<ndim>::iterator::next() {
	if (pos >= dw.num_semiwords)
		return;

	pos++;
	intrablock_pos = (intrablock_pos + CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD) % BITS_PER_BLOCK;
	block_pos = pos / SEMIWORDS_PER_BLOCK;
}

template<int ndim>
void
CBLQSemiwords<ndim>::iterator::set(cblq_semiword_t semiword) {
	if (!has_top()) {
		dw.num_semiwords++;
		// Make sure there's a next slot for the semiword
		if (dw.num_semiwords > dw.semiwords.size() * SEMIWORDS_PER_BLOCK)
			dw.semiwords.push_back(0);
	}

	// Update the given semiword
	block_t &block = dw.semiwords[block_pos];
	block &= ~((block_t)SEMIWORD_MASK << intrablock_pos);
	block |= ((block_t)semiword << intrablock_pos);
}

template<int ndim>
void
CBLQSemiwords<ndim>::iterator::set_fullword(cblq_word_t fullword) {
	assert(((dw.encoding_two_codes ? CBLQRegionEncoding<ndim>::ONE_CODES_WORD : CBLQRegionEncoding<ndim>::TWO_CODES_WORD) & fullword) == 0);

	const cblq_word_t shifted = fullword >> (dw.encoding_two_codes ? 1 : 0);
	const cblq_semiword_t undilated = (cblq_semiword_t)Dilater<cblq_word_t, 2>::undilate(shifted); // NOTE: undilating 0b0101 == 0b11
	this->set(undilated);
}

template<int ndim>
void
CBLQSemiwords<ndim>::trim(size_t new_num_semiwords) {
	assert(new_num_semiwords <= num_semiwords);

	num_semiwords = new_num_semiwords; // Logically, this is the only modification needed
	semiwords.resize((num_semiwords + SEMIWORDS_PER_BLOCK - 1) / SEMIWORDS_PER_BLOCK); // Trim the underlying vector as well, though, to save memory
	semiwords.shrink_to_fit();
}

template<int ndim>
void
CBLQSemiwords<ndim>::append(CBLQSemiwords &other) {
	assert(encoding_two_codes == other.encoding_two_codes);

	const int semiword_shift = num_semiwords % SEMIWORDS_PER_BLOCK;
	if (semiword_shift == 0) {
		semiwords.resize(num_semiwords / SEMIWORDS_PER_BLOCK); // trim to exact length
		semiwords.insert(semiwords.end(), other.semiwords.begin(), other.semiwords.end()); // append new words
	} else {
		// Back of semiwords: LLLLLLL???
		// Iter in otherwords: HHHLLLLLLL
		const int low_bits = semiword_shift * CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD;
		const int hi_bits = BITS_PER_BLOCK - low_bits;
		const block_t low_mask = (1ULL << low_bits) - 1;
		const block_t hi_mask = (1ULL << hi_bits) - 1; // Masks "hi_bits" of the *least significant bits* (i.e., to get the high hi_bits, shift the mask by low_bits)

		for (auto other_it = other.semiwords.begin(); other_it != other.semiwords.end(); other_it++) {
			const block_t other_block = *other_it;

			semiwords.back() = (semiwords.back() & low_mask) | (other_block & hi_mask) << low_bits;
			semiwords.push_back((other_block & (low_mask << hi_bits)) >> hi_bits);
		}
	}

	num_semiwords += other.num_semiwords;
}

template<int ndim>
auto CBLQSemiwords<ndim>::get(size_t pos)
	-> cblq_semiword_t
{
	const size_t bit_pos = pos * CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD;
	const size_t block_pos = bit_pos / BITS_PER_BLOCK;
	const int intrablock_pos = (bit_pos % BITS_PER_BLOCK);

	// Update the given semiword
	const block_t block = this->semiwords[block_pos];
	return (block >> intrablock_pos) & SEMIWORD_MASK;
}

template<int ndim>
void
CBLQSemiwords<ndim>::set(size_t pos, cblq_semiword_t value) {
	const size_t bit_pos = pos * CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD;
	const size_t block_pos = bit_pos / BITS_PER_BLOCK;
	const int intrablock_pos = (bit_pos % BITS_PER_BLOCK);

	// Update the given semiword
	block_t &block = this->semiwords[block_pos];
	block &= ~((block_t)SEMIWORD_MASK << intrablock_pos);
	block |= ((block_t)value << intrablock_pos);
}

template<int ndim>
void
CBLQSemiwords<ndim>::ensure_padded() {
	// If the number of semiwords first exactly into the number of blocks we have, pad with one empty block
	if (semiwords.size() * SEMIWORDS_PER_BLOCK == num_semiwords)
		semiwords.push_back(0);
}

// Expands this semiword array to the given size, with new elements having undefined values
template<int ndim>
void
CBLQSemiwords<ndim>::expand(size_t new_num_semiwords) {
	assert(new_num_semiwords >= num_semiwords);

	num_semiwords = new_num_semiwords; // Logically, this is the only modification needed
	semiwords.resize((num_semiwords + SEMIWORDS_PER_BLOCK - 1) / SEMIWORDS_PER_BLOCK); // Expand the underlying buffer to accommodate
}

template<int ndim>
void
CBLQSemiwords<ndim>::clear() {
	this->semiwords.clear();
	this->num_semiwords = 0;
}

template<int ndim>
void
CBLQSemiwords<ndim>::dump() const {
	int mod = 0;
	iterator it = const_cast<CBLQSemiwords*>(this)->begin(); // H4x, since this is const; too lazy to fix this right for now
	while (it.has_top()) {
		CBLQRegionEncoding<ndim>::print_cblq_semiword(it.top());
		if ((++mod %= 16) == 0)
			printf("\n");
		else
			printf(" ");
		it.next();
	}
}

template<int ndim>
bool
CBLQSemiwords<ndim>::operator==(const CBLQSemiwords<ndim> &other) {
	if (this->num_semiwords != other.num_semiwords)
		return false;

	const uint64_t full_blocks = (this->num_semiwords / CBLQSemiwords<ndim>::SEMIWORDS_PER_BLOCK);
	const int semiwords_in_last_block = (this->num_semiwords % CBLQSemiwords<ndim>::SEMIWORDS_PER_BLOCK);
	const uint64_t last_block_mask = (1ULL << (semiwords_in_last_block * CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD)) - 1;

	for (uint64_t i = 0; i < full_blocks; i++)
		if (this->semiwords[i] != other.semiwords[i])
			return false;

	if (semiwords_in_last_block > 0)
		if ((this->semiwords[full_blocks] & last_block_mask) !=
			(other.semiwords[full_blocks] & last_block_mask))
			return false;

	return true;
}

// Explicit instantiation of CBLQSemiwords for 2D-4D
template class CBLQSemiwords<2>;
template class CBLQSemiwords<3>;
template class CBLQSemiwords<4>;
