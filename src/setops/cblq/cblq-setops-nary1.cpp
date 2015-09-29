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
 * cblq-setops-nary1.cpp
 *
 *  Created on: Mar 25, 2014
 *      Author: David A. Boyuka II
 */

#include <vector>
#include <deque>
#include <queue>

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/iterator/counting_iterator.hpp>

#include "pique/region/cblq/cblq.hpp"
#include "pique/setops/cblq/cblq-setops.hpp"

extern "C" {
#include "setops/cblq/cblq-setops-actiontables.h"
}

template<int ndim>
boost::shared_ptr< CBLQRegionEncoding<ndim> >
CBLQSetOperationsNAry1<ndim>::nary_set_op_impl(RegionEncodingCPtrCIter cblq_it, RegionEncodingCPtrCIter cblq_end_it, NArySetOperation op) const {
	// Convert the external N-ary set operation enumeration to the internal action table constant
	// TODO: Find a better way to do this
	nary_action_t first_action;
	switch (op) {
	case NArySetOperation::UNION:
		first_action = nary_action_t::N_UNION;
		break;
	case NArySetOperation::INTERSECTION:
		first_action = nary_action_t::N_INTER;
		break;
	case NArySetOperation::DIFFERENCE:
		first_action = nary_action_t::N_DIFF;
		break;
	case NArySetOperation::SYMMETRIC_DIFFERENCE:
		first_action = nary_action_t::N_SYMDIFF;
		break;
	default:
		abort();
	}

	// Initialize iterators to traverse through the words of each CBLQ operand passed to this function (as well as the implicit "this" CBLQ)
	const uint64_t domain_size = (*cblq_it)->get_domain_size();
	size_t num_operands = 0;
	std::vector< typename std::vector<cblq_word_t>::const_iterator > cblq_word_its;
	std::vector< typename std::vector<cblq_word_t>::const_iterator > cblq_word_end_its;
	for (; cblq_it != cblq_end_it; cblq_it++) {
		const CBLQRegionEncoding<ndim> &cblq = **cblq_it; // first * = deref iterator, second * = deref pointer to CBLQ that the iterator was holding
		num_operands++;
		cblq_word_its.push_back(cblq.words.cbegin());
		cblq_word_end_its.push_back(cblq.words.cend());
	}

	// Allocate a new CBLQ for output
	boost::shared_ptr< CBLQRegionEncoding<ndim> > output = boost::make_shared< CBLQRegionEncoding<ndim> >(domain_size);
	output->has_dense_suffix = false;

	std::vector<cblq_word_t> &output_words = output->words;

	// Allocate the requisite state queues for this operation
	std::queue<nary_action_t> action_queue;
	std::queue<size_t> opercount_queue;
	std::deque<size_t> oper_queue(boost::counting_iterator<size_t>(0),
								  boost::counting_iterator<size_t>(num_operands));

	action_queue.push(first_action);
	opercount_queue.push(num_operands);

	// Enter the main loop
	// NOTE: prefix "cur" means "current" as in "the current state", and is always const
	// NOTE: prefix "next" means "next to be (possibly) output", and is never const
	// NOTE: "oper" means "operand", and refers to one of the CBLQ operands.
	//       "index" means "index within the operand list for this action", whereas simply "oper" means "index within the original operand list"
	// TODO: Evaluate transposed inner loop: process all codes for a single operand, produce an enqueue bitmap, and aggregate bitmaps into args/argcount at the end
	while (action_queue.size()) {
		// Get the next action to apply
		const nary_action_t cur_word_action = action_queue.front();
		const size_t cur_opercount = opercount_queue.front();

		cblq_word_t next_output_word = 0;

		// Apply the action to each code position across all touched operands
		for (int code_bitpos = 0; code_bitpos < CBLQRegionEncoding<ndim>::BITS_PER_WORD; code_bitpos += CBLQRegionEncoding<ndim>::BITS_PER_CODE) {
			nary_action_t next_output_action = cur_word_action;
			cblq_word_t next_output_code = NARY_ACTION_INITIAL_CODE[cur_word_action];
			size_t next_output_opercount = 0;

//			size_t oper_index = 0;
//			for (auto oper_it = oper_queue.begin(); oper_index < cur_opercount; oper_it++, oper_index++) {
			// BUGFIX: use direct indexing, since iterators can be invalidated by the push_back inside this loop
			for (size_t oper_index = 0; oper_index < cur_opercount; oper_index++) {
				const size_t oper = oper_queue[oper_index];

				const cblq_word_t cur_oper_word = *cblq_word_its[oper];
				const cblq_word_t cur_oper_code = (cur_oper_word >> code_bitpos) & 0b11;

				// (next action, next output code, current operand code) -> (new next action, new next output code)
				const nary_action_table_entry_t &transition = NARY_ACTION_TABLE[next_output_action][next_output_code][cur_oper_code];

				// Transition to next action and output code
				next_output_action = transition.action;
				next_output_code = transition.output;

				// If applying the action to the current code requires future work to be enqueued, do so now
				if (cur_oper_code & CBLQRegionEncoding<ndim>::TWO_CODES_WORD) {
					oper_queue.push_back(oper);
					next_output_opercount++;
				}
			}

			// Add the final output code to the current output word, and enqueue another action if any operand requires it
			next_output_word |= (next_output_code << code_bitpos);
			if (next_output_opercount) {
				action_queue.push(next_output_action);
				opercount_queue.push(next_output_opercount);
				// at least one appropriate opers was already pushed to oper_queue in the above loop
			} else {
				// no opers were pushed to oper_queue in the above loop; thus, all 3 queues remain unchanged
			}
		}

		// Push the completed output word into the output CBLQ
		// (unless the current action is DELETE, in which case it is discarded)
		if (cur_word_action != nary_action_t::N_DELETE)
			output_words.push_back(next_output_word);

		// Advance all input operands that were drawn from
		size_t oper_index = 0;
		for (auto oper_it = oper_queue.begin(); oper_index < cur_opercount; oper_it++, oper_index++) {
			const size_t oper = *oper_it;

			assert(cblq_word_its[oper] != cblq_word_end_its[oper]); // Just checkin'
			cblq_word_its[oper]++;
		}

		// Finally, pop the just-completed action and associated parameters from their respective queues
		action_queue.pop();
		opercount_queue.pop();
		oper_queue.erase(oper_queue.begin(), oper_queue.begin() + cur_opercount);
	}

	// Ensure we end in a consistent state
	assert(!action_queue.size() && !oper_queue.size() && !opercount_queue.size());

	// Finally, compact the output CBLQ if requested
	if (this->conf.compact_after_setop)
		output->compact();

	return output;
}

// Explicit instantiation of 2D-4D CBLQ N-ary set operations
template class CBLQSetOperationsNAry1<2>;
template class CBLQSetOperationsNAry1<3>;
template class CBLQSetOperationsNAry1<4>;
