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
 * cii-setops.cpp
 *
 *  Created on: Mar 22, 2014
 *      Author: David A. Boyuka II
 */

#include <vector>
#include <queue>

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/region/cblq/cblq.hpp"
#include "pique/setops/cblq/cblq-setops.hpp"

extern "C" {
#include "setops/cblq/cblq-setops-actiontables.h"
}

template<int ndim>
static inline
typename CBLQRegionEncoding<ndim>::cblq_word_t
apply_unary_action(const cblq_code_action_t op, typename CBLQRegionEncoding<ndim>::cblq_word_t word, std::queue<cblq_code_action_t> &out_queue) {
	typedef typename CBLQRegionEncoding<ndim>::cblq_word_t cblq_word_t;

    const action_table_entry_t (&action_table)[3] = UNARY_ACTION_TABLES[op];
    const int bits_per_word = CBLQRegionEncoding<ndim>::BITS_PER_WORD;
    const int bits_per_code = CBLQRegionEncoding<ndim>::BITS_PER_CODE;
    const cblq_word_t code_mask = (1 << bits_per_code) - 1;

    cblq_word_t out_word_val = 0;
    for (int bitpos = 0; bitpos < bits_per_word; bitpos += bits_per_code) {
        const int code = word & code_mask;
        word >>= bits_per_code;

        const action_table_entry_t &transition = action_table[code];
        const cblq_word_t output = transition.output;
        const cblq_code_action_t next_action = transition.action;

        out_word_val |= (output << bitpos);
        if (next_action != cblq_code_action_t::NO_OP)
            out_queue.push(next_action);
    }

    return out_word_val;
}

template<int ndim>
static inline
typename CBLQRegionEncoding<ndim>::cblq_word_t
apply_binary_action(const cblq_code_action_t op, typename CBLQRegionEncoding<ndim>::cblq_word_t left_word, typename CBLQRegionEncoding<ndim>::cblq_word_t right_word, std::queue<cblq_code_action_t> &out_queue) {
	typedef typename CBLQRegionEncoding<ndim>::cblq_word_t cblq_word_t;

	const action_table_entry_t (&action_table)[3][3] = BINARY_ACTION_TABLES[op];
    const int bits_per_word = CBLQRegionEncoding<ndim>::BITS_PER_WORD;
    const int bits_per_code = CBLQRegionEncoding<ndim>::BITS_PER_CODE;
    const cblq_word_t code_mask = ((cblq_word_t)1 << bits_per_code) - 1;

    cblq_word_t out_word_val = 0;
    for (int bitpos = 0; bitpos < bits_per_word; bitpos += bits_per_code) {
    	const int left_code = left_word & code_mask;
    	const int right_code = right_word & code_mask;
        left_word >>= bits_per_code;
        right_word >>= bits_per_code;

        const action_table_entry_t &transition = action_table[left_code][right_code];
        const cblq_word_t output = transition.output;
        const cblq_code_action_t next_action = transition.action;

        out_word_val |= (output << bitpos);
        if (next_action != cblq_code_action_t::NO_OP)
            out_queue.push(next_action);
    }

    return out_word_val;
}

template<int ndim>
boost::shared_ptr< CBLQRegionEncoding<ndim> >
CBLQSetOperations<ndim>::unary_set_op_impl(boost::shared_ptr< const CBLQRegionEncoding<ndim> > region, UnarySetOperation op) const {
	// NOTE: This is the implementation from CBLQSetOperationsFast. One might expect the action-table-based approach here,
	// but I was too lazy to implement it, especially since this is clearly faster, and there is only one conceivably useful
	// unary set operation on a region (complement), so a hard-coded approach has no real drawback.
	using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;

	const bool has_dense_suffix = region->has_dense_suffix;

	boost::shared_ptr< CBLQRegionEncoding<ndim> > output = boost::make_shared< CBLQRegionEncoding<ndim> >(region->get_domain_size());
	output->level_lens = region->level_lens;
	output->has_dense_suffix = region->has_dense_suffix;

	auto &out_cblq = output->words;
	auto &out_dense = *output->dense_suffix;
	auto &in_cblq = region->words;
	auto &in_dense = *region->dense_suffix;

	out_cblq.resize(in_cblq.size());

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
			for (auto in_it = in_dense.begin(), out_it = out_dense.begin(); in_it.has_top(); in_it.next(), out_it.next()) {
				typename CBLQSemiwords<ndim>::cblq_semiword_t semiword = in_it.top();
				semiword ^= CBLQRegionEncoding<ndim>::FULL_SEMIWORD;
				out_it.set(semiword);
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
CBLQSetOperations<ndim>::binary_set_op_impl(boost::shared_ptr< const CBLQRegionEncoding<ndim> > left, boost::shared_ptr< const CBLQRegionEncoding<ndim> > right, NArySetOperation op) const {
	assert(left->get_domain_size() == right->get_domain_size());
	assert(left->get_num_levels() == right->get_num_levels());
	//assert(left->has_dense_suffix == right->has_dense_suffix); // Only supports both dense or both non-dense

	// Map the RegionEncoding set operation to a corresponding initial action from the action table
	// TODO: Find a better way to do this mapping
	cblq_code_action_t first_action;
	switch (op) {
	case NArySetOperation::UNION:
		first_action = cblq_code_action_t::UNION;
		break;
	case NArySetOperation::INTERSECTION:
		first_action = cblq_code_action_t::INTERSECT;
		break;
	case NArySetOperation::DIFFERENCE:
		first_action = cblq_code_action_t::DIFFERENCE;
		break;
	case NArySetOperation::SYMMETRIC_DIFFERENCE:
		first_action = cblq_code_action_t::SYM_DIFFERENCE;
		break;
	default:
		abort();
	}

	const boost::optional< bool > common_has_dense_suffix = CBLQRegionEncoding<ndim>::deduce_common_suffix_density(*left, *right);
	assert(common_has_dense_suffix); // both operands must have compatible suffix density (i.e., the same density, or at least one empty)

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

	auto left_it = left_cblq.cbegin();
	auto right_it = right_cblq.cbegin();
	auto left_it_end = left_cblq.cend();
	auto right_it_end = right_cblq.cend();

	// Push the initial action into the queue
    std::queue<cblq_code_action_t> actionqueue;
    actionqueue.push(first_action);

    // Double-nested for loop: the first counts which level of the CBLQ we are on, while the second iterates within a level
    // Could be replaced by a single loop for now, but future methods such as vague prefix/exact suffix will need this
    for (int level = 0; level < non_dense_levels; level++) {
    	const size_t level_len = actionqueue.size();
    	const size_t words_before_level = output_cblq.size();

    	for (size_t queuepos = 0; queuepos < level_len; queuepos++) {
    		cblq_code_action_t nextop = (cblq_code_action_t)actionqueue.front();
    		actionqueue.pop();

    		//printf("Next op: %d\n", nextop);

    		switch (nextop) {
    		case cblq_code_action_t::NO_OP:
    			break;

    		case cblq_code_action_t::DELETE_1:
    		case cblq_code_action_t::COPY_1:
    		case cblq_code_action_t::COMPLEMENT_1:
    		{
    			const cblq_word_t output_word = apply_unary_action<ndim>(nextop, *left_it++, actionqueue);
    			if (nextop != cblq_code_action_t::DELETE_1)
    				output_cblq.push_back(output_word);
    			break;
    		}

    		case cblq_code_action_t::DELETE_2:
    		case cblq_code_action_t::COPY_2:
    		case cblq_code_action_t::COMPLEMENT_2:
    		{
    			const cblq_word_t output_word = apply_unary_action<ndim>(nextop, *right_it++, actionqueue);
    			if (nextop != cblq_code_action_t::DELETE_2)
    				output_cblq.push_back(output_word);
    			break;
    		}

    		case cblq_code_action_t::UNION:
    		case cblq_code_action_t::INTERSECT:
    		case cblq_code_action_t::DIFFERENCE:
    		case cblq_code_action_t::SYM_DIFFERENCE:
    		{
    			const cblq_word_t output_word = apply_binary_action<ndim>(nextop, *left_it++, *right_it++, actionqueue);
    			output_cblq.push_back(output_word);
    			break;
    		}
    		}
    	}

    	const size_t words_after_level = output_cblq.size();
    	output->level_lens[level] = words_after_level - words_before_level;
    }

    assert(left_it == left_it_end && right_it == right_it_end);

    if (has_dense_suffix){
    	output->level_lens[levels - 1] = 0; // Record 0 non-dense words in the lowest (finest) level

    	typename CBLQSemiwords<ndim>::iterator left_it = left->dense_suffix->begin();
    	typename CBLQSemiwords<ndim>::iterator right_it = right->dense_suffix->begin();
    	typename CBLQSemiwords<ndim>::iterator output_it = output->dense_suffix->begin();
    	while (!actionqueue.empty()) {
    		cblq_code_action_t nextop = (cblq_code_action_t)actionqueue.front();
    		actionqueue.pop();

    		//printf("Next op: %d\n", nextop);

    		switch (nextop) {
    		// "Naughtary" ops
    		case cblq_code_action_t::NO_OP:
    			break;

    		// Unary ops
    		case cblq_code_action_t::DELETE_1: left_it.next(); break;
    		case cblq_code_action_t::DELETE_2: right_it.next(); break;
    		case cblq_code_action_t::COPY_1:
    			output_it.set(left_it.top());
    			output_it.next();
    			left_it.next();
    			break;
    		case cblq_code_action_t::COPY_2:
    			output_it.set(right_it.top());
    			output_it.next();
    			right_it.next();
    			break;

    		case cblq_code_action_t::COMPLEMENT_1:
    			output_it.set(~left_it.top());
    			output_it.next();
    			left_it.next();
    			break;
    		case cblq_code_action_t::COMPLEMENT_2:
    			output_it.set(~right_it.top());
    			output_it.next();
    			right_it.next();
    			break;

    		// Binary ops
    		case cblq_code_action_t::UNION:
    			output_it.set(left_it.top() | right_it.top());
    			output_it.next();
    			left_it.next();
    			right_it.next();
    			break;
    		case cblq_code_action_t::INTERSECT:
    			output_it.set(left_it.top() & right_it.top());
    			output_it.next();
    			left_it.next();
    			right_it.next();
    			break;
    		case cblq_code_action_t::DIFFERENCE:
    			output_it.set(left_it.top() & ~right_it.top());
    			output_it.next();
    			left_it.next();
    			right_it.next();
    			break;
    		case cblq_code_action_t::SYM_DIFFERENCE:
    			output_it.set(left_it.top() ^ right_it.top());
    			output_it.next();
    			left_it.next();
    			right_it.next();
    			break;
    		}
    	}

    	assert(!left_it.has_top() && !right_it.has_top());
    }

    if (this->conf.compact_after_setop && !this->suppress_compact)
    	output->compact();

    return output;
}

template<int ndim>
boost::shared_ptr< CBLQRegionEncoding<ndim> >
CBLQSetOperations<ndim>::nary_set_op_impl(RegionEncodingCPtrCIter region_it, RegionEncodingCPtrCIter region_end_it, NArySetOperation op) const {
	this->suppress_compact = this->conf.no_compact_during_nary;

	boost::shared_ptr< CBLQRegionEncoding<ndim> > output = this->nary_set_op_default_impl(region_it, region_end_it, op);

	this->suppress_compact = false;
	if (this->conf.no_compact_during_nary && this->conf.compact_after_setop)
		output->compact();

	return output;
}

// Explicit instantiation of 2D-4D CBLQ set ops
template class CBLQSetOperations<2>;
template class CBLQSetOperations<3>;
template class CBLQSetOperations<4>;
