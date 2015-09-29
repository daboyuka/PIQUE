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
 * cblq-setops-nary2.cpp
 *
 *  Created on: Mar 25, 2014
 *      Author: David A. Boyuka II
 */

#include <iostream>
#include <algorithm>
#include <iterator>
#include <vector>

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/iterator/counting_iterator.hpp>

#include "pique/region/cblq/cblq.hpp"
#include "pique/setops/setops.hpp"
#include "pique/setops/cblq/cblq-setops.hpp"

struct child_ind_helper {
	typedef uint16_t lookup_index_t;
	static constexpr int BITS_PER_CODE = 2;
	static constexpr int BITS_PER_LOOKUP = sizeof(lookup_index_t) * 8;
	static constexpr int CODES_PER_LOOKUP = BITS_PER_LOOKUP / BITS_PER_CODE;
	static constexpr uint64_t LOOKUP_ENTRIES = 1ULL << BITS_PER_LOOKUP;
	static constexpr lookup_index_t LOOKUP_MASK = ~(lookup_index_t)0;

	uint64_t children[CODES_PER_LOOKUP];
	int nchildren;

	static child_ind_helper CHILD_IND_TABLE[LOOKUP_ENTRIES];
	static bool CHILD_IND_TABLE_INIT;

	static void ensure_init();
};

bool child_ind_helper::CHILD_IND_TABLE_INIT = false;
child_ind_helper child_ind_helper::CHILD_IND_TABLE[LOOKUP_ENTRIES];
void child_ind_helper::ensure_init() {
	if (CHILD_IND_TABLE_INIT)
		return;

	for (uint64_t word = 0; word < LOOKUP_ENTRIES; word++) {
		uint64_t *out = CHILD_IND_TABLE[word].children;
		for (int i = 0; i < CODES_PER_LOOKUP; i++) {
			if (word & (0b10 << i * BITS_PER_CODE)) {// Two code
				*out = i;
				++out;
			}
		}
		CHILD_IND_TABLE[word].nchildren = out - CHILD_IND_TABLE[word].children;
	}
	CHILD_IND_TABLE_INIT = true;
}

template<int ndim>
inline static int count_two_codes(typename CBLQRegionEncoding<ndim>::cblq_word_t word) {
	static constexpr int NUM_LOOKUPS = (CBLQRegionEncoding<ndim>::CODES_PER_WORD - 1) / child_ind_helper::CODES_PER_LOOKUP + 1;

	int children = 0;
	for (int i = 0; i < NUM_LOOKUPS; ++i) {
		const typename child_ind_helper::lookup_index_t lookup_key = ((word >> (i * child_ind_helper::BITS_PER_LOOKUP)) & child_ind_helper::LOOKUP_MASK);
		children += child_ind_helper::CHILD_IND_TABLE[lookup_key].nchildren;
	}

	return children;
}

template<int ndim>
inline static void append_child_inds(typename CBLQRegionEncoding<ndim>::cblq_word_t word, uint64_t base_ind, typename std::vector< int64_t >::iterator &out_inds_it) {
	static constexpr int NUM_LOOKUPS = (CBLQRegionEncoding<ndim>::CODES_PER_WORD - 1) / child_ind_helper::CODES_PER_LOOKUP + 1;

	if (CBLQRegionEncoding<ndim>::CODES_PER_WORD <= child_ind_helper::CODES_PER_LOOKUP) {
		const child_ind_helper &lookup = child_ind_helper::CHILD_IND_TABLE[word];

		const uint64_t *child_ind_offset = lookup.children;
		const int nchildren = lookup.nchildren;

		for (int i = 0; i < CBLQRegionEncoding<ndim>::CODES_PER_WORD; ++i)
			out_inds_it[i] = base_ind + child_ind_offset[i]; // This will overflow the valid end of out_level_inds, but we allocated enough extra sentinel space earlier so that it won't segfault

		out_inds_it += nchildren;
	} else {
		for (int lookupnum = 0; lookupnum < NUM_LOOKUPS; ++lookupnum) {
			const typename child_ind_helper::lookup_index_t lookup_key = (word >> (lookupnum * child_ind_helper::BITS_PER_LOOKUP)) & child_ind_helper::LOOKUP_MASK;
			const child_ind_helper &lookup = child_ind_helper::CHILD_IND_TABLE[lookup_key];

			const uint64_t *child_ind_offset = lookup.children;
			const int nchildren = lookup.nchildren;

			for (int i = 0; i < child_ind_helper::CODES_PER_LOOKUP; ++i)
				out_inds_it[i] = base_ind + child_ind_offset[i] + lookupnum * child_ind_helper::CODES_PER_LOOKUP;

			out_inds_it += nchildren;
		}
	}
}

// Handles postprocessing after iterating over a CBLQ level:
// 1. Densely remaps inds produced by the level for use during the next level
// 2. Maps any 3-codes into 1-codes
// 3. Computes the level_len for the output CBLQ's next level
template<int ndim, NArySetOperation op>
inline static void level_postprocess(
		typename std::vector< typename CBLQRegionEncoding<ndim>::cblq_word_t >::iterator next_level_word_it,
		const std::vector< uint64_t > &out_level_inds_operand_ends,
		std::vector< int64_t > &inds /* in: output of prev level, out: input of next level */,
		uint64_t &level_len /* in: prev level, out: next level */)
{
	static const int64_t CHUNK_SIZE = 1ULL << 13;
	using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;

	assert(CHUNK_SIZE % CBLQRegionEncoding<ndim>::CODES_PER_WORD == 0);

	// First, compute the compaction mapping for the outinds (remove all indices pointing to NO-OP and DELETE actions)
	const int64_t max_ind = level_len * CBLQRegionEncoding<ndim>::CODES_PER_WORD; // One ind per code in the output of this level
	std::vector< int64_t > ind_mapping;

	const size_t num_operands = out_level_inds_operand_ends.size();

	int64_t mapped_ind = 0;

	auto scan_word_it = next_level_word_it;

	std::vector< typename std::vector<int64_t>::iterator > ind_update_its;
	std::vector< typename std::vector<int64_t>::iterator > ind_update_end_its;
	for (auto operand_end_it = out_level_inds_operand_ends.begin(); operand_end_it != out_level_inds_operand_ends.end(); ++operand_end_it) {
		uint64_t operand_end_off = *operand_end_it;
		ind_update_its.push_back(ind_update_end_its.empty() ? inds.begin() : ind_update_end_its.back());
		ind_update_end_its.push_back(inds.begin() + operand_end_off);
	}

	for (int64_t chunk_begin_ind = 0; chunk_begin_ind < max_ind; chunk_begin_ind += CHUNK_SIZE) {
		const int64_t chunk_end_ind = std::min(chunk_begin_ind + CHUNK_SIZE, max_ind);
		const int64_t cur_chunk_size = chunk_end_ind - chunk_begin_ind;
		ind_mapping.clear();
		ind_mapping.resize(cur_chunk_size, -1); // Initially, all inds map to -1 (i.e. DELETE)

		for (int64_t cur_ind = 0; cur_ind < cur_chunk_size; ++scan_word_it) {
			cblq_word_t word = *scan_word_it;
			if (op == NArySetOperation::UNION) {
				word &= ~((word & CBLQRegionEncoding<ndim>::ONE_CODES_WORD) << 1); // Fix 3-codes -> 1-codes
			} else if (op == NArySetOperation::INTERSECTION) {
				// FF->FF
				// FT->FT
				// TF->FF
				// TT->TF

				// XY &= Y1, XY &= 1(~X)

				word &= (word << 1) | CBLQRegionEncoding<ndim>::ONE_CODES_WORD;
				word &= (~word >> 1) | CBLQRegionEncoding<ndim>::TWO_CODES_WORD;
			} else
				abort();
			*scan_word_it = word;

			for (int codepos = 0; codepos < CBLQRegionEncoding<ndim>::CODES_PER_WORD; ++codepos, ++cur_ind) {
				if (word & 0b10 << (codepos * CBLQRegionEncoding<ndim>::BITS_PER_CODE))
					ind_mapping[cur_ind] = mapped_ind++;
			}
		}

		// Densely remap all outinds produced
		for (int operand = 0; operand < num_operands; operand++) {
			auto ind_update_it = ind_update_its[operand];
			auto ind_update_end_it = ind_update_end_its[operand];
			for (; ind_update_it != ind_update_end_it; ++ind_update_it) {
				const int64_t ind = *ind_update_it;
				if (ind >= 0) {
					if (ind >= chunk_end_ind)
						break;
					else
						*ind_update_it = ind_mapping[ind - chunk_begin_ind];
				}
			}
			ind_update_its[operand] = ind_update_it;
		}
	}

	level_len = mapped_ind; // Exclusive upper bound on all mapped outinds
}

template<int ndim>
template<NArySetOperation op>
inline boost::shared_ptr< CBLQRegionEncoding<ndim> >
CBLQSetOperationsNAry3Fast<ndim>::nary_set_op_impl(RegionEncodingCPtrCIter cblq_it, RegionEncodingCPtrCIter cblq_end_it) const {
	const boost::optional< bool > common_has_dense_suffix = CBLQRegionEncoding<ndim>::deduce_common_suffix_density(cblq_it, cblq_end_it);
	assert(common_has_dense_suffix); // all operands must have compatible suffix density (i.e., each the same/empty)

	const int levels = (*cblq_it)->get_num_levels();
	const bool has_dense_suffix = *common_has_dense_suffix;
	const int non_dense_levels = has_dense_suffix ? levels - 1 : levels;
	const uint64_t domain_size = (*cblq_it)->get_domain_size();

	// Initialize iterators to traverse through the words of each CBLQ operand passed to this function (as well as the implicit "this" CBLQ)
	size_t num_operands = 0;
	std::vector< typename std::vector<cblq_word_t>::const_iterator > cblq_word_its;
	std::vector< typename std::vector<cblq_word_t>::const_iterator > cblq_word_end_its;
	std::vector< typename std::vector<uint64_t>::const_iterator > cblq_level_len_its;
	for (auto it = cblq_it; it != cblq_end_it; it++) {
		const CBLQRegionEncoding<ndim> &cblq = **it; // first * = deref iterator, second * = deref pointer to CBLQ that the iterator was holding
		// Ensure uniform level count and suffix denseness across all CBLQs
		assert(cblq.get_num_levels() == levels);
		//assert(cblq.has_dense_suffix == has_dense_suffix); // now checked above
		assert(cblq.domain_size == domain_size);

		num_operands++;
		cblq_word_its.push_back(cblq.words.cbegin());
		cblq_word_end_its.push_back(cblq.words.cend());
		cblq_level_len_its.push_back(cblq.level_lens.cbegin());
	}

	// Allocate a new CBLQ for output
	boost::shared_ptr< CBLQRegionEncoding<ndim> > output = boost::make_shared< CBLQRegionEncoding<ndim> >(domain_size);
	output->has_dense_suffix = has_dense_suffix;
	output->level_lens.resize(levels);

	std::vector<cblq_word_t> &output_words = output->words;

	// Input operand target index array. Produced in the previous step, indicates which output CBLQ word should be modified by each input word
	// Outind arrays are packed back-to-back, since it is known how many belong to each operand (= operand.level_lens[level])
	std::vector<int64_t> in_level_inds(num_operands, 0); // Fill with 0 indices for each operand

	// Output operand target index array. Built up during processing of a level, then dense-compacted and moved to in_level_outinds
	std::vector<int64_t> out_level_inds;
	std::vector<uint64_t> out_level_inds_operand_ends(num_operands);

	// Size of level for the next iteration (computed at the end of each iteration for the next iteration)
	size_t next_level_len = 1;

	// For each CBLQ level
	for (int level = 0; level < non_dense_levels; level++) {
		output->level_lens[level] = next_level_len; // The number of words we will output this level is already known: it's the number of non-delete actions that are queued

		// Allocate space in out_level_inds
		if (level + 1 < levels) {
			uint64_t expected_out_inds = 0; // The number of inds produced by the next level is
			if (level + 1 < non_dense_levels) { // Next level is the dense suffix
				for (size_t operand = 0; operand < num_operands; ++operand)
					expected_out_inds += *(cblq_level_len_its[operand] + 1); // Add the number of words in the next level
			} else {
				for (auto it = cblq_it; it != cblq_end_it; ++it)
					expected_out_inds += (*it)->dense_suffix->get_num_semiwords(); // Add the number of semiwords in the next level
			}
			out_level_inds.resize(expected_out_inds + CBLQRegionEncoding<ndim>::CODES_PER_WORD /* Extra sentinel to allow intentional overrun */);
		} else {
			out_level_inds.resize(CBLQRegionEncoding<ndim>::CODES_PER_WORD /* Extra sentinel to allow intentional overrun */);
		}

		// Allocate space in output_words and set up the base iterator
		const size_t prev_output_words_size = output_words.size();
		output_words.resize(prev_output_words_size + next_level_len, (op == NArySetOperation::UNION) ? CBLQRegionEncoding<ndim>::ZERO_CODES_WORD : CBLQRegionEncoding<ndim>::ONE_CODES_WORD);
		const auto level_output_words_it = output_words.begin() + prev_output_words_size;

		// Set up in and out ind iterators
		auto in_inds_it = in_level_inds.cbegin();
		auto out_inds_it = out_level_inds.begin();

		// For each operand, apply all CBLQ words to the proper locations in the output (or skip them if they are to be deleted)
		for (size_t operand = 0; operand < num_operands; operand++) {
			auto &cblq_level_len_it = cblq_level_len_its[operand];
			const uint64_t level_len = *cblq_level_len_it;
			++cblq_level_len_it;

			auto cblq_word_it = cblq_word_its[operand]; // Get the CBLQ word iterator for this CBLQ
			auto cblq_word_end_it = cblq_word_it + level_len;

			// For each action in which this operand participates
			while (cblq_word_it != cblq_word_end_it) {
				const int64_t ind = *in_inds_it;
				++in_inds_it;

				if (ind >= 0) { // BINARY SETOP
					cblq_word_t oper_cblq_word = *cblq_word_it;
					++cblq_word_it;

					if (op == NArySetOperation::UNION) {
						level_output_words_it[ind] |= oper_cblq_word; // Works for all cases but 1 union 2 resulting in 3. This is cleaned up later
					} else if (op == NArySetOperation::INTERSECTION) {
						level_output_words_it[ind] |= (oper_cblq_word & CBLQRegionEncoding<ndim>::TWO_CODES_WORD); // Add any 2 codes from oper_cblq_word to output
						level_output_words_it[ind] &= (oper_cblq_word | (oper_cblq_word >> 1) | CBLQRegionEncoding<ndim>::TWO_CODES_WORD); // Convert 1-codes to 0-codes and 3-codes to 2-codes for any 0-code in the input
					} else
						abort();

					const uint64_t base_ind = ind * CBLQRegionEncoding<ndim>::CODES_PER_WORD;
					append_child_inds<ndim>(oper_cblq_word, base_ind, out_inds_it); // This will overflow the valid end of out_level_inds, but we allocated enough extra sentinel space earlier so that it won't segfault
				} else { // DELETE
					// Skip the specified number of words, counting the number of two codes therein
					int64_t skip_children = 0;
					for (int64_t i = 0; i < -ind; ++i) {
						skip_children += count_two_codes<ndim>(*cblq_word_it);
						++cblq_word_it;
					}
					// If there were any skipped two codes, enqueue a delete action covering all of them now
					if (skip_children) {
						*out_inds_it = -skip_children;
						++out_inds_it;
					}
				}
			}

			cblq_word_its[operand] = cblq_word_it;
			out_level_inds_operand_ends[operand] = out_inds_it - out_level_inds.begin();
		}

		// Post-process the output of the previous step
		if (level < levels - 1) {
			out_level_inds.resize(out_level_inds.size() - CBLQRegionEncoding<ndim>::CODES_PER_WORD); // Remove the extra sentinel for overflow so that the vector is tight (level_postprocess relies on an accurate size())
			level_postprocess<ndim, op>(level_output_words_it, out_level_inds_operand_ends, out_level_inds, next_level_len); // updates out_level_inds and next_level_len
			in_level_inds = std::move(out_level_inds);
			out_level_inds.clear();
		}
	}

	if (has_dense_suffix) {
		using semiword_block_t = typename CBLQSemiwords<ndim>::block_t;
		const int BITS_PER_BLOCK = std::numeric_limits<semiword_block_t>::digits;

		// Set the (non-dense) leaf level length to 0
		output->level_lens[levels - 1] = 0;

		// Collect dense suffix information and iterators
		std::vector< uint64_t > dense_level_lens;
		std::vector< typename std::vector< semiword_block_t >::const_iterator > in_block_its;
		for (auto it = cblq_it; it != cblq_end_it; it++) {
			const CBLQRegionEncoding<ndim> &cblq = **it;
			dense_level_lens.push_back(cblq.dense_suffix->get_num_semiwords());
			cblq.dense_suffix->ensure_padded();
			in_block_its.push_back(cblq.dense_suffix->semiwords.begin());
		}

		// Set up output
		CBLQSemiwords<ndim> &output_semiwords = *output->dense_suffix;
		std::vector< semiword_block_t > &output_blocks = output_semiwords.semiwords;

		output_semiwords.num_semiwords = next_level_len;
		output_blocks.clear();
		output_blocks.resize(next_level_len / CBLQSemiwords<ndim>::SEMIWORDS_PER_BLOCK + 1, 0);

		// Process dense suffixes
		auto in_inds_it = in_level_inds.begin();
		for (size_t operand = 0; operand < num_operands; ++operand) {
			const uint64_t dense_level_len = dense_level_lens[operand];
			auto in_block_it = in_block_its[operand];

			semiword_block_t cur_block = *in_block_it;
			uint64_t in_block_shift = 0;
			for (uint64_t i = 0; i < dense_level_len;) {
				const int64_t ind = *in_inds_it;
				++in_inds_it;

				if (ind >= 0) {
					// Compute intersection via De Morgan's Law (negation of union of negations). This is the inner negation.
					const semiword_block_t semiword =
							op == NArySetOperation::UNION ?
									((cur_block >> in_block_shift) & CBLQSemiwords<ndim>::SEMIWORD_MASK) :
									(((~cur_block) >> in_block_shift) & CBLQSemiwords<ndim>::SEMIWORD_MASK);

					const uint64_t ind_block = ind / CBLQSemiwords<ndim>::SEMIWORDS_PER_BLOCK;
					const int ind_shift = ind % CBLQSemiwords<ndim>::SEMIWORDS_PER_BLOCK * CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD;

					output_blocks[ind_block] |= (semiword << ind_shift);

					++i;
					in_block_shift += CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD;
					if (in_block_shift == BITS_PER_BLOCK) {
						cur_block = *++in_block_it;
						in_block_shift = 0;
					}
				} else {
					const int64_t skip = -ind;
					in_block_shift += (skip * CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD);
					i += skip;

					if (in_block_shift >= BITS_PER_BLOCK) {
						in_block_it += in_block_shift / BITS_PER_BLOCK;
						in_block_shift = in_block_shift % BITS_PER_BLOCK;
						cur_block = *in_block_it;
					}
				}
			}
		}

		// Compute intersection via De Morgan's Law (negation of union of negations). This is the outer negation.
		if (op == NArySetOperation::INTERSECTION) {
			for (auto it = output_blocks.begin(); it != output_blocks.end(); ++it)
				*it = ~*it;
		}
	}

	return output;
}

template<int ndim>
boost::shared_ptr< CBLQRegionEncoding<ndim> >
CBLQSetOperationsNAry3Fast<ndim>::nary_set_op_impl(RegionEncodingCPtrCIter cblq_it, RegionEncodingCPtrCIter cblq_end_it, NArySetOperation op) const {
	child_ind_helper::ensure_init();

	boost::shared_ptr< CBLQRegionEncoding<ndim> > output;
	if (op == NArySetOperation::UNION)
		output = this->nary_set_op_impl<NArySetOperation::UNION>(cblq_it, cblq_end_it);
	else if (op == NArySetOperation::INTERSECTION)
		output = this->nary_set_op_impl<NArySetOperation::INTERSECTION>(cblq_it, cblq_end_it);
	else
		return this->nary_set_op_default_impl(cblq_it, cblq_end_it, op);

	// Finally, compact the output CBLQ if requested
	if (this->conf.compact_after_setop)
		output->compact();

	return output;
}

//template<int ndim>
//boost::shared_ptr< CBLQRegionEncoding<ndim> >
//CBLQSetOperationsNAry3Fast<ndim>::binary_set_op(boost::shared_ptr< const CBLQRegionEncoding<ndim> > left, boost::shared_ptr< const CBLQRegionEncoding<ndim> > right, NArySetOperation op) const {
//	std::vector< boost::shared_ptr< CBLQRegionEncoding<ndim> > > operands = {
//		boost::const_pointer_cast< CBLQRegionEncoding<ndim> >(left),
//		boost::const_pointer_cast< CBLQRegionEncoding<ndim> >(right)
//	};
//
//	if (op == NArySetOperation::UNION || op == NArySetOperation::INTERSECTION)
//		return this->template CBLQSetOperationsNAry3Fast<ndim>::nary_set_op(operands.begin(), operands.end(), op);
//	else
//		return this->template CBLQSetOperationsFast<ndim>::binary_set_op(left, right, op);
//}

// Explicit instantiation of 2D-4D CBLQ N-ary set operations
template class CBLQSetOperationsNAry3Fast<2>;
template class CBLQSetOperationsNAry3Fast<3>;
template class CBLQSetOperationsNAry3Fast<4>;
