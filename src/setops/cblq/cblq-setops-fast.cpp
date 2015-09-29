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
 * cblq-setops-fast.cpp
 *
 *  Created on: Jun 11, 2014
 *      Author: David A. Boyuka II
 */

#include <type_traits>
#include <vector>

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/optional.hpp>

#include "pique/region/cblq/cblq.hpp"
#include "pique/setops/cblq/cblq-setops.hpp"

// Special values needed for optimization below
// -2 -> 0 | 2, -1 -> 1 | 2, 0 -> 2 | 2, 1 -> 2 | 1, 2 -> 2 | 0
// -2 -> COPY_R, -1 -> DELETE_R, 0 -> UNION, 1 -> DELETE_L, 2 -> COPY_L
enum struct cblq_union_action_t { COPY_R = -2, DELETE_R = -1, UNION = 0, DELETE_L = 1, COPY_L = 2, };

struct cblq_action_block_t {
	cblq_action_block_t() : action(cblq_union_action_t::UNION), count(0) {}
	cblq_action_block_t(cblq_union_action_t action, size_t count) :
		action(action), count(count)
	{}
	cblq_union_action_t action;
	size_t count;
};

template<int ndim>
inline static int count_two_codes(typename CBLQRegionEncoding<ndim>::cblq_word_t word) {
	int count = 0;
	for (int i = 0; i < CBLQRegionEncoding<ndim>::CODES_PER_WORD; i++) {
		count += (word & 0b10);
		word >>= CBLQRegionEncoding<ndim>::BITS_PER_CODE;
	}
	return count >> 1; // Divide by 2, since each 2-code added 2
}

template<int ndim, cblq_union_action_t inaction>
inline static void apply_unary_action_helper(
		size_t count,
		typename std::vector< typename CBLQRegionEncoding<ndim>::cblq_word_t >::const_iterator &in_it,
		typename std::vector< typename CBLQRegionEncoding<ndim>::cblq_word_t >::iterator &out_it,
		typename std::vector< cblq_action_block_t >::iterator &actionqueue_out_it)
{
	typedef typename CBLQRegionEncoding<ndim>::cblq_word_t cblq_word_t;
	size_t children = 0;
	do {
		const cblq_word_t in_word = *in_it;
		++in_it;
		if (inaction == cblq_union_action_t::COPY_L || inaction == cblq_union_action_t::COPY_R) {
			*out_it = in_word;
			++out_it;
		}
		children += count_two_codes<ndim>(in_word);
	} while (--count > 0);

	if (children) {
		*actionqueue_out_it = cblq_action_block_t(inaction, children);
		++actionqueue_out_it;
	}
}

template<int ndim>
inline static void apply_union_helper(
		typename std::vector< typename CBLQRegionEncoding<ndim>::cblq_word_t >::const_iterator &left_it,
		typename std::vector< typename CBLQRegionEncoding<ndim>::cblq_word_t >::const_iterator &right_it,
		typename std::vector< typename CBLQRegionEncoding<ndim>::cblq_word_t >::iterator &out_it,
		typename std::vector< cblq_action_block_t >::iterator &actionqueue_out_it)
{
	typedef typename CBLQRegionEncoding<ndim>::cblq_word_t cblq_word_t;

	cblq_word_t left_word = *left_it;
	cblq_word_t right_word = *right_it;
	++left_it;
	++right_it;

	cblq_word_t or_word = left_word | right_word; // 0/1 U 0/1 -> 0/1, 0 U 2 -> 2, 2 U 2 -> 2, 1 U 2 -> 3 (fix this)
	const cblq_word_t out_word = or_word & ~((or_word & CBLQRegionEncoding<ndim>::ONE_CODES_WORD) << 1); // Masks out 2's with a mask created from the 1's
	*out_it = out_word;
	++out_it;

	// Search for 2-codes in both words in parallel, and queue actions if found
	for (int codepos = 0; codepos < CBLQRegionEncoding<ndim>::CODES_PER_WORD; codepos++) {
		if (or_word & 0b10) {
			// -2 -> 0 | 2, -1 -> 1 | 2, 0 -> 2 | 2, 1 -> 2 | 1, 2 -> 2 | 0
			// -2 -> COPY_R, -1 -> DELETE_R, 0 -> UNION, 1 -> DELETE_L, 2 -> DELETE_R
			const cblq_union_action_t codediff_action = (cblq_union_action_t)((int64_t)(left_word & 0b11) - (int64_t)(right_word & 0b11));
			*actionqueue_out_it = cblq_action_block_t(codediff_action, 1);
			++actionqueue_out_it;
		}
		if (codepos < CBLQRegionEncoding<ndim>::CODES_PER_WORD - 1) {
			left_word >>= CBLQRegionEncoding<ndim>::BITS_PER_CODE;
			right_word >>= CBLQRegionEncoding<ndim>::BITS_PER_CODE;
			or_word >>= CBLQRegionEncoding<ndim>::BITS_PER_CODE;
		}
	}
}

template<typename block_t>
inline static void copy_bits(
		const size_t count,
		typename std::vector< block_t >::const_iterator &in_bits, block_t &in_headblock, int &in_shift_remaining,
		typename std::vector< block_t >::iterator &out_bits, block_t &out_headblock, int &out_shift_remaining)
{
	const int BITS_PER_BLOCK = (sizeof(block_t) << 3);
	ptrdiff_t scount = (ptrdiff_t)count;
	do {
		out_headblock |= (in_headblock << (BITS_PER_BLOCK - out_shift_remaining));

		ptrdiff_t bits_used = scount;
		if (out_shift_remaining < bits_used)
			bits_used = out_shift_remaining;
		if (in_shift_remaining < bits_used)
			bits_used = in_shift_remaining;

		if (bits_used == out_shift_remaining) {
			*out_bits = out_headblock;
			++out_bits;
			out_shift_remaining = BITS_PER_BLOCK;
			out_headblock = 0;
		} else {
			out_shift_remaining -= bits_used;
			out_headblock &= (~(block_t)0) >> out_shift_remaining; // Mask out excess bits
		}

		if (bits_used == in_shift_remaining) {
			++in_bits;
			in_headblock = *in_bits;
			in_shift_remaining = BITS_PER_BLOCK;
		} else {
			in_shift_remaining -= bits_used;
			in_headblock >>= bits_used;
		}

		scount -= bits_used;
	} while (scount > 0);
}

template<typename block_t>
inline static void skip_bits(
		const size_t count,
		typename std::vector< block_t >::const_iterator &in_bits, block_t &in_headblock, int &in_shift_remaining)
{
	const int BITS_PER_BLOCK = (sizeof(block_t) << 3);
	ptrdiff_t scount = (ptrdiff_t)count;
	do {
		if (in_shift_remaining <= scount) { // in_shift_remaining is the limiting factor
			scount -= in_shift_remaining;
			in_shift_remaining = BITS_PER_BLOCK;
			++in_bits;
			in_headblock = *in_bits;
		} else { // scount is the limiting factor
			in_shift_remaining -= scount;
			in_headblock >>= scount;
			break;
		}
	} while (scount > 0);
}

template<int ndim, typename semiword_block_t>
inline static void binary_union_set_op(
		bool has_dense_suffix, int non_dense_levels,
		typename std::vector< typename CBLQRegionEncoding<ndim>::cblq_word_t >::const_iterator &left_it,
		typename std::vector< typename CBLQRegionEncoding<ndim>::cblq_word_t >::const_iterator &right_it,
		typename std::vector< typename CBLQRegionEncoding<ndim>::cblq_word_t >::iterator &out_it,
		typename std::vector< semiword_block_t>::const_iterator &left_denseit,
		typename std::vector< semiword_block_t>::const_iterator &right_denseit,
		typename std::vector< semiword_block_t>::iterator &out_denseit,
		int &out_dense_remaining_bits,
		std::vector<size_t> &out_level_lens)
{
	typedef typename CBLQRegionEncoding<ndim>::cblq_word_t cblq_word_t;
	std::vector< cblq_action_block_t > this_actionqueue;
	std::vector< cblq_action_block_t > next_actionqueue = { cblq_action_block_t(cblq_union_action_t::UNION, 1) };

    for (int level = 0; level < non_dense_levels; level++) {
    	this_actionqueue = std::move(next_actionqueue);
    	next_actionqueue.clear();
    	next_actionqueue.resize(this_actionqueue.size() * CBLQRegionEncoding<ndim>::CODES_PER_WORD + 1); // All cblq_action_block_t's default to is_copy=false and left/right_count = 0
    	auto next_actionqueue_out_it = next_actionqueue.begin();

    	//const size_t level_len = this_actionqueue.size(); // not needed
    	const typename std::vector<cblq_word_t>::iterator out_it_before_level = out_it;

    	for (auto it = this_actionqueue.cbegin(), end_it = this_actionqueue.cend(); it != end_it; ++it) {
    		const cblq_action_block_t &curaction = *it;
    		const cblq_union_action_t action = curaction.action;
    		const size_t count = curaction.count;

    		switch (action) {
    		case cblq_union_action_t::UNION:
    			apply_union_helper<ndim>(left_it, right_it, out_it, next_actionqueue_out_it);
    			break;
    		case cblq_union_action_t::DELETE_L:
    			apply_unary_action_helper<ndim, cblq_union_action_t::DELETE_L>(count, left_it, out_it, next_actionqueue_out_it);
    			break;
    		case cblq_union_action_t::DELETE_R:
    			apply_unary_action_helper<ndim, cblq_union_action_t::DELETE_R>(count, right_it, out_it, next_actionqueue_out_it);
    			break;
    		case cblq_union_action_t::COPY_L:
    			apply_unary_action_helper<ndim, cblq_union_action_t::COPY_L>(count, left_it, out_it, next_actionqueue_out_it);
    			break;
    		case cblq_union_action_t::COPY_R:
    			apply_unary_action_helper<ndim, cblq_union_action_t::COPY_R>(count, right_it, out_it, next_actionqueue_out_it);
    			break;
    		}
    	}

    	const size_t num_output_words = out_it - out_it_before_level;
    	const size_t num_actions = next_actionqueue_out_it - next_actionqueue.begin();

    	out_level_lens[level] = num_output_words;
    	next_actionqueue.resize(num_actions);
    }

    if (has_dense_suffix) {
    	const int BITS_PER_BLOCK = (sizeof(semiword_block_t) << 3);
    	// Invariants: left/right/out_denseit always points to where the corresponding headblock came from/is going to
    	semiword_block_t left_headblock = *left_denseit;
    	semiword_block_t right_headblock = *right_denseit;
    	semiword_block_t out_headblock = *out_denseit;
    	int left_shift_remaining = BITS_PER_BLOCK;
    	int right_shift_remaining = BITS_PER_BLOCK;
    	int out_shift_remaining = BITS_PER_BLOCK;

    	//typename std::remove_reference<decltype(out_denseit)>::type			out_denseit_save;
    	//typename std::remove_reference<decltype(out_shift_remaining)>::type	out_shift_remaining_save;

    	for (auto it = next_actionqueue.cbegin(), end_it = next_actionqueue.cend(); it != end_it; ++it) {
    		const cblq_action_block_t &curaction = *it;
    		const cblq_union_action_t action = curaction.action;
    		const size_t count_in_bits = curaction.count * CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD;

    		switch (action) {
    		case cblq_union_action_t::UNION:
    			// For union, just copy both left and right, since they OR into the same buffer
    			/* no break */
    		case cblq_union_action_t::COPY_L:
    			// copy (OR) the bits from the left buffer into the out buffer
    			copy_bits(count_in_bits, left_denseit, left_headblock, left_shift_remaining, out_denseit, out_headblock, out_shift_remaining);
    			if (action == cblq_union_action_t::COPY_L)
    				break;

    			// If we get here, ut was really UNION, so reset the out pointer and fall through to COPY_R
    			if (out_shift_remaining == BITS_PER_BLOCK) { // We just wrapped around to a new block (since UNION always has count exactly 1)
    				--out_denseit;
    				out_headblock = *out_denseit;
    				out_shift_remaining = CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD;
    			} else {
    				out_shift_remaining += CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD;
    			}
    			/* no break */
    		case cblq_union_action_t::COPY_R:
    			// copy (OR) the bits from the right buffer into the out buffer
    			copy_bits(count_in_bits, right_denseit, right_headblock, right_shift_remaining, out_denseit, out_headblock, out_shift_remaining);
    			break;
    		case cblq_union_action_t::DELETE_L:
    			// skip the appropriate number of bits in the left buffer
    			skip_bits(count_in_bits, left_denseit, left_headblock, left_shift_remaining);
    			break;
    		case cblq_union_action_t::DELETE_R:
    			// skip the appropriate number of bits in the right buffer
    			skip_bits(count_in_bits, right_denseit, right_headblock, right_shift_remaining);
    			break;
    		}
    	}

    	*out_denseit = out_headblock; // Copy any remaining bits; don't worry about overflow garbage bits
    	++out_denseit; // Advance past this last (partial) block; "out_dense_remaining_bits" tells how many bits to disregard in this block
    	out_dense_remaining_bits = out_shift_remaining;
    }
}

template<int ndim>
boost::shared_ptr< CBLQRegionEncoding<ndim> >
CBLQSetOperationsFast<ndim>::unary_set_op_impl(boost::shared_ptr< const CBLQRegionEncoding<ndim> > region, UnarySetOperation op) const {
	using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;
	using semiword_block_t = typename CBLQSemiwords<ndim>::block_t;

	const bool has_dense_suffix = region->has_dense_suffix;

	boost::shared_ptr< CBLQRegionEncoding<ndim> > output = boost::make_shared< CBLQRegionEncoding<ndim> >(region->get_domain_size());
	output->level_lens = region->level_lens;
	output->has_dense_suffix = region->has_dense_suffix;

	auto &out_cblq = output->words;
	auto &out_dense = *output->dense_suffix;
	auto &in_cblq = region->words;
	auto &in_dense = *region->dense_suffix;

	out_cblq.resize(in_cblq.size());
	if (has_dense_suffix) {
		out_dense.num_semiwords = in_dense.num_semiwords;
		out_dense.semiwords.resize(in_dense.semiwords.size());
	}

	switch (op) {
	case UnarySetOperation::COMPLEMENT:
	{
		auto in_it = in_cblq.cbegin();
		auto out_it = out_cblq.begin();
		for (; in_it != in_cblq.cend(); ++in_it, ++out_it) {
			const cblq_word_t word = *in_it;
			*out_it = word ^ ((word & CBLQRegionEncoding<ndim>::TWO_CODES_WORD) >> 1) ^ CBLQRegionEncoding<ndim>::ONE_CODES_WORD; // 0->1, 1->0, 2->2
		}

		if (has_dense_suffix) {
			for (auto in_it = in_dense.semiwords.begin(), out_it = out_dense.semiwords.begin(); in_it != in_dense.semiwords.end(); ++in_it, ++out_it) {
				*out_it = ~*in_it;
			}
		}

		break;
	}
	default:
		abort();
	}

	return output;
}

template<int ndim>
boost::shared_ptr< CBLQRegionEncoding<ndim> >
CBLQSetOperationsFast<ndim>::binary_set_op_impl(boost::shared_ptr< const CBLQRegionEncoding<ndim> > left, boost::shared_ptr< const CBLQRegionEncoding<ndim> > right, NArySetOperation op) const {
	if (op != NArySetOperation::UNION)
		return this->template CBLQSetOperations<ndim>::binary_set_op_impl(left, right, op); // Delegate upward if this isn't a union

	assert(left->get_domain_size() == right->get_domain_size());
	assert(left->get_num_levels() == right->get_num_levels());

	const boost::optional< bool > common_has_dense_suffix = CBLQRegionEncoding<ndim>::deduce_common_suffix_density(*left, *right);
	assert(common_has_dense_suffix); // Only supports both dense, both non-dense, or when at least one operand has an empty suffix

	const int levels = left->get_num_levels();
	const bool has_dense_suffix = *common_has_dense_suffix;
	const int non_dense_levels = has_dense_suffix ? levels - 1 : levels;

	// Allocate an output CBLQRegionEncoding
	boost::shared_ptr< CBLQRegionEncoding<ndim> > output = boost::make_shared< CBLQRegionEncoding<ndim> >(left->get_domain_size());
	output->has_dense_suffix = has_dense_suffix;
	output->level_lens.resize(levels);

	// Capture the input CBLQ word deques, and extract iterators
	const std::vector<cblq_word_t> &left_cblq = left->words;
	const std::vector<cblq_word_t> &right_cblq = right->words;
	std::vector<cblq_word_t> &output_cblq = output->words;
	output_cblq.resize(left_cblq.size() + right_cblq.size()); // Worst case, it is the sum of both

	CBLQSemiwords<ndim> &left_dense = *left->dense_suffix;
	CBLQSemiwords<ndim> &right_dense = *right->dense_suffix;
	CBLQSemiwords<ndim> &out_dense = *output->dense_suffix;
	if (has_dense_suffix) {
		// Pad the left and right semiword buffer, since we may access one block beyond the end
		// Note: appending a new block has no effect on the content of a CBLQSemiwords, as actual length is tracked separately
		left_dense.ensure_padded();
		right_dense.ensure_padded();
		// Resize and clear to 0 the output semiword buffer
		out_dense.semiwords.clear();
		out_dense.semiwords.resize(left_dense.semiwords.size() + right_dense.semiwords.size(), 0);
	}

	auto left_it = left_cblq.cbegin();
	auto right_it = right_cblq.cbegin();
	auto out_it = output_cblq.begin();
	auto left_denseit = left_dense.semiwords.cbegin();
	auto right_denseit = right_dense.semiwords.cbegin();
	auto out_denseit = out_dense.semiwords.begin();
	int out_dense_remaining_bits;

	switch (op) {
	case NArySetOperation::UNION:
		binary_union_set_op< ndim, typename CBLQSemiwords<ndim>::block_t >(has_dense_suffix, non_dense_levels, left_it, right_it, out_it, left_denseit, right_denseit, out_denseit, out_dense_remaining_bits, output->level_lens);
		break;
	default:
		abort();
	}

	assert(left_it == left_cblq.cend());
	assert(right_it == right_cblq.cend());
	output_cblq.resize(out_it - output_cblq.begin()); // Trim to length

	if (has_dense_suffix) {
		out_dense.num_semiwords = (out_denseit - out_dense.semiwords.begin()) * CBLQSemiwords<ndim>::SEMIWORDS_PER_BLOCK -
				(out_dense_remaining_bits / CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD); // All full blocks, minus any remaining bit space in the last block
		out_dense.trim(out_dense.num_semiwords);
	}

    if (this->conf.compact_after_setop)
    	output->compact();

    return output;
}

// Explicit instantiation of 2D-4D CBLQ set ops
template class CBLQSetOperationsFast<2>;
template class CBLQSetOperationsFast<3>;
template class CBLQSetOperationsFast<4>;
