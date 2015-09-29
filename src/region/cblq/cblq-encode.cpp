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
 * cblq-encode-impl.hpp
 *
 *  Created on: Jan 28, 2014
 *      Author: drew
 */

#include "pique/util/dbprintf.hpp"

#include "pique/util/compute-exp-level.hpp"
#include "pique/region/cblq/cblq-encode.hpp"

// Constructor

template<int ndim>
CBLQRegionEncoder<ndim>::CBLQRegionEncoder(CBLQRegionEncoderConfig conf, size_t total_elements) :
	RegionEncoder< CBLQRegionEncoding<ndim>, CBLQRegionEncoderConfig >(conf, ((size_t)1) << (compute_exp_level<ndim>(total_elements) * ndim)),
	nlayers(compute_exp_level<ndim>(total_elements)),
	nelem(total_elements),

	word_count(0),
	layer_words(nlayers),
	dense_suffix_semiwords(false),

	cur_words(nlayers),
	cur_word_lens(nlayers)
{}

// Functions

template<int ndim>
inline void CBLQRegionEncoder<ndim>::push_bits_impl(uint64_t count, bool bitval) {
	typedef CBLQRegionEncoding<ndim> CBLQOut;

    if (count == 0)
        return;

    const cblq_word_t mixed_code = 0b10;
    const cblq_word_t set_code = bitval ? 0b01 : 0b00;
    const cblq_word_t set_mask = bitval ? CBLQOut::ONE_CODES_WORD : CBLQOut::ZERO_CODES_WORD;

    dbprintf("\nPushing %llu %d-bits...\n", count, bitval ? 1 : 0);

    bool carry = false;
    bool carry_pure_word = false;
    for (int layer = 0; layer < this->nlayers; layer++) {
        cblq_word_t &cur_word     = this->cur_words[layer];
        uint64_t    &cur_word_len = this->cur_word_lens[layer];

        dbprintf("Pushing %llu bits into layer %d\n", count, layer);

        // If there is a carry, append it first
        if (layer > 0) {
            const cblq_word_t code = carry_pure_word ? set_code : mixed_code;
            dbprintf("Appending previous carry of code %hhu...\n", code);

            cur_word |= (code) << (cur_word_len * CBLQOut::BITS_PER_CODE);
            cur_word_len++;

            // BUGFIX: This prevents the left-shift from occurring when cur_word_len == CODES_PER_WORD,
            // which results in an undefined shift operation
            if (cur_word_len < CBLQOut::CODES_PER_WORD) {
            	// Append all set codes to the end of the word (will be trimmed later, if needed)
            	cur_word |= set_mask << (cur_word_len * CBLQOut::BITS_PER_CODE); // Fill the remainder of the word with the set bit value
            }
        } else {
        	// BUGFIX: part of the above fix
        	// Append all set codes to the end of the word (will be trimmed later, if needed)
        	cur_word |= set_mask << (cur_word_len * CBLQOut::BITS_PER_CODE); // Fill the remainder of the word with the set bit value
        }

        cur_word_len += count;

        dbprintf("Increased word length to %llu, filled remainder of word with set code:", cur_word_len);
#ifdef DEBUG
        CBLQOut::print_cblq_word(cur_word);
#endif
        dbprintf("\n");

        // If there are at least enough pushed codes to fill the word, perform a carry
        // Else, do not carry, and terminate after trimming the word
        if (cur_word_len >= CBLQOut::CODES_PER_WORD) {
            carry = true;
            // Whether the entire word is homogeneous (all 1-codes or 0-codes)
            // It is enough to compare to the bit value being pushed; it's not
            // possible to get a homogeneous word of the opposite code because
            // we're pushing or carrying at least one code.
            carry_pure_word = (cur_word == set_mask);

            dbprintf("Triggered carry, pure? %s\n", carry_pure_word ? "true" : "false");

            // If this word isn't pure (and thus will be fully represented by
            // its parent node), OR this is the highest layer (in which case
            // we cannot carry a pure word), we append it to this layer
            if (!carry_pure_word || layer == (this->nlayers - 1)) {
            	if (this->conf.encode_dense_suffix && layer == 0)
            		this->dense_suffix_semiwords.push_fullword(cur_word);
            	else
            		this->layer_words[layer].push_back(cur_word);

                this->word_count++;

                dbprintf("Pushed word onto layer %d: ", layer);
#ifdef DEBUG
                CBLQOut::print_cblq_word(cur_word);
#endif
                dbprintf("\n");
            }

            // Set the current word/word length to the "tail" of the push
            cur_word = set_mask;
            count = cur_word_len / CBLQOut::CODES_PER_WORD - 1; // - 1 because the first filled code is the carry, and is handled separately
            cur_word_len %= CBLQOut::CODES_PER_WORD; // Note: guarantees (cur_word_len < CBLQOut::CODES_PER_WORD)

            dbprintf("Push tail length on layer %d: %d, push length to next layer: %llu\n", layer, cur_word_len, count);
        } else {
            carry = false;
        }

        // Trim overflowing codes from the word in any case

        //const int codes_to_erase = CBLQOut::CODES_PER_WORD - cur_word_len;
        //cur_word &= ((cblq_word_t)-1) >> (codes_to_erase * CBLQOut::BITS_PER_CODE);
        // FIXED HORRIBLE BUG: the above code may fail when (codes_to_erase * CBLQOut::BITS_PER_CODE) == (num bits in word),
        // because in C++, the result of a bit shift greater *OR EQUAL* to the number of bits in a type is *UNDEFINED*
        // However, we know (cur_word_len < CBLQOut::CODES_PER_WORD) due to the if() and % above, so we can just shift left instead of right

        cur_word &= (((cblq_word_t)1) << (cur_word_len * CBLQOut::BITS_PER_CODE)) - 1;

        dbprintf("Current word trimmed to %d codes: \n", cur_word_len);
#ifdef DEBUG
        CBLQOut::print_cblq_word(cur_word);
#endif
        dbprintf("\n");

        // If there is no carry, we are done
        if (!carry)
            break;
    }
}

template<int ndim>
inline boost::shared_ptr< CBLQRegionEncoding<ndim> > CBLQRegionEncoder<ndim>::to_region_encoding() {
    boost::shared_ptr< CBLQRegionEncoding<ndim> > cblq =
    		boost::make_shared< CBLQRegionEncoding<ndim> >(this->nelem);

    std::vector<cblq_word_t> &output = cblq->words;

    for (int i = this->nlayers - 1; i >= 0; i--) {
        const std::vector<cblq_word_t> &layer = this->layer_words[i];

        output.insert(output.end(), layer.begin(), layer.end());
        cblq->level_lens.push_back(this->layer_words[i].size());
    }

    cblq->has_dense_suffix = this->conf.encode_dense_suffix;
    if (this->conf.encode_dense_suffix) {
    	cblq->dense_suffix->clear();
    	cblq->dense_suffix->append(this->dense_suffix_semiwords); // TODO: This full copy could be optimized away
    }

    return cblq;
}

// Explicit instantiation for 2D-4D CBLQ region encoders
template class CBLQRegionEncoder<2>;
template class CBLQRegionEncoder<3>;
template class CBLQRegionEncoder<4>;
