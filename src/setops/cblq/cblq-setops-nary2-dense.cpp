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

#include <vector>
#include <queue>

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/iterator/counting_iterator.hpp>

#include "pique/region/cblq/cblq.hpp"
#include "pique/setops/setops.hpp"
#include "pique/setops/cblq/cblq-setops.hpp"

extern "C" {
#include "setops/cblq/cblq-setops-actiontables.h"
}

template<int ndim>
boost::shared_ptr< CBLQRegionEncoding<ndim> >
CBLQSetOperationsNAry2Dense<ndim>::nary_set_op_impl(RegionEncodingCPtrCIter cblq_it, RegionEncodingCPtrCIter cblq_end_it, NArySetOperation op) const {
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
	for (auto it = cblq_it; it != cblq_end_it; it++) {
		const CBLQRegionEncoding<ndim> &cblq = **it; // first * = deref iterator, second * = deref pointer to CBLQ that the iterator was holding
		// Ensure uniform level count and suffix denseness across all CBLQs
		assert(cblq.get_num_levels() == levels);
		//assert(cblq.has_dense_suffix == has_dense_suffix); // now checked above

		num_operands++;
		cblq_word_its.push_back(cblq.words.cbegin());
		cblq_word_end_its.push_back(cblq.words.cend());
	}

	// Allocate a new CBLQ for output
	boost::shared_ptr< CBLQRegionEncoding<ndim> > output = boost::make_shared< CBLQRegionEncoding<ndim> >(domain_size);
	output->has_dense_suffix = has_dense_suffix;
	output->level_lens.resize(levels);

	std::vector<cblq_word_t> &output_words = output->words;

	// Allocate the requisite state queues for this operation
	std::queue<nary_action_t> action_queue;
	std::queue< std::vector<size_t> > operset_queue;

	action_queue.push(first_action);
	operset_queue.emplace(boost::counting_iterator<size_t>(0), boost::counting_iterator<size_t>(num_operands));

	std::vector<nary_action_t> next_output_actions(CBLQRegionEncoding<ndim>::CODES_PER_WORD);
	std::vector< std::vector<size_t> > next_output_opersets(CBLQRegionEncoding<ndim>::CODES_PER_WORD);

	// Enter the main loop
	// NOTE: prefix "cur" means "current" as in "the current state", and is always const
	// NOTE: prefix "next" means "next to be (possibly) output", and is never const
	// NOTE: "oper" means "operand", and refers to one of the CBLQ operands.
	//       "index" means "index within the operand list for this action", whereas simply "oper" means "index within the original operand list"
	for (int level = 0; level < non_dense_levels; level++) {
		const size_t level_len = action_queue.size();
		const size_t words_before_level = output_words.size();
		for (size_t actioni = 0; actioni < level_len; actioni++) {
			// Get the next action to apply
			const nary_action_t cur_word_action = action_queue.front();
			const std::vector<size_t> &cur_operset = operset_queue.front();

			// Initialize the outputs for this action
			cblq_word_t next_output_word = NARY_ACTION_INITIAL_CODE[cur_word_action] ? CBLQRegionEncoding<ndim>::ONE_CODES_WORD : CBLQRegionEncoding<ndim>::ZERO_CODES_WORD;
			next_output_actions.assign(CBLQRegionEncoding<ndim>::CODES_PER_WORD, cur_word_action);
			// Assume next_output_opers[i] are all cleared

			// Transition the output state with each operand
			for (auto oper_it = cur_operset.begin(); oper_it != cur_operset.end(); oper_it++) {
				const size_t cur_oper = *oper_it;
				const cblq_word_t cur_oper_word = *cblq_word_its[cur_oper]++;
				const cblq_word_t last_next_output_word = next_output_word;
				next_output_word = 0;

				for (int code_pos = 0, code_bitpos = 0; code_pos < CBLQRegionEncoding<ndim>::CODES_PER_WORD; code_pos++, code_bitpos += CBLQRegionEncoding<ndim>::BITS_PER_CODE) {
					const cblq_word_t cur_oper_code = (cur_oper_word >> code_bitpos) & 0b11;

					const cblq_word_t next_output_code = (last_next_output_word >> code_bitpos) & 0b11;
					nary_action_t &next_output_action = next_output_actions[code_pos];

					// (next action, next output code, current operand code) -> (new next action, new next output code)
					const nary_action_table_entry_t &transition = NARY_ACTION_TABLE[next_output_action][next_output_code][cur_oper_code];

					// Transition to next action and output code
					next_output_action = transition.action;
					next_output_word |= transition.output << code_bitpos;

					// If applying the action to the current code requires future work to be enqueued (exactly when
					// the current code is a two-code), do so now
					if (cur_oper_code & CBLQRegionEncoding<ndim>::TWO_CODES_WORD) {
						std::vector<size_t> &next_output_operset = next_output_opersets[code_pos];
						next_output_operset.push_back(cur_oper);
					}
				}
			}

			// Push the completed output word into the output CBLQ
			// (unless the current action is DELETE, in which case it is discarded)
			if (cur_word_action != nary_action_t::N_DELETE)
				output_words.push_back(next_output_word);

			// Enqueue the next actions/opersets. Only enqueue an action/operset if it has a non-empty operset
			auto next_output_action_it = next_output_actions.cbegin();
			auto next_output_operset_it = next_output_opersets.begin();
			for (; next_output_action_it != next_output_actions.cend(); next_output_action_it++, next_output_operset_it++) {
				std::vector<size_t> &next_output_operset = *next_output_operset_it;

				if (next_output_operset.size()) {
					action_queue.push(*next_output_action_it);

					operset_queue.push(std::move(next_output_operset));
					next_output_operset.clear();
				}
			}

			// Finally, pop the just-completed action and associated parameters from their respective queues
			action_queue.pop();
			operset_queue.pop();
		}

		const size_t words_after_level = output_words.size();
		output->level_lens[level] = words_after_level - words_before_level;
	}

	if (has_dense_suffix) {
		output->level_lens[levels - 1] = 0;

		typename CBLQSemiwords<ndim>::iterator output_semiword_iter = output->dense_suffix->begin();
		std::vector< typename CBLQSemiwords<ndim>::iterator > semiword_iters;
		for (auto it = cblq_it; it != cblq_end_it; it++) {
			CBLQRegionEncoding<ndim> &cblq = const_cast<CBLQRegionEncoding<ndim> &>(**it); // const-ness hack
			semiword_iters.push_back(cblq.dense_suffix->begin());
		}

		while (!action_queue.empty()) {
			nary_action_t cur_word_action = action_queue.front();
			const std::vector<size_t> &cur_operset = operset_queue.front();

			if (cur_word_action == nary_action_t::N_DELETE) {
				for (auto operset_it = cur_operset.begin(); operset_it != cur_operset.end(); operset_it++) {
					const size_t cur_oper = *operset_it;
					typename CBLQSemiwords<ndim>::iterator &oper_iter = semiword_iters[cur_oper];
					oper_iter.next();
				}
			} else {
				cblq_semiword_t output_word = NARY_ACTION_INITIAL_CODE[cur_word_action] ? CBLQRegionEncoding<ndim>::FULL_SEMIWORD : CBLQRegionEncoding<ndim>::EMPTY_SEMIWORD;

				for (auto operset_it = cur_operset.begin(); operset_it != cur_operset.end(); operset_it++) {
					const size_t cur_oper = *operset_it;
					typename CBLQSemiwords<ndim>::iterator &oper_iter = semiword_iters[cur_oper];

					const cblq_semiword_t cur_oper_word = oper_iter.top();
					oper_iter.next();

					switch (cur_word_action) {
					case nary_action_t::N_UNION:
						output_word |= cur_oper_word;
						break;
					case nary_action_t::N_INTER:
						output_word &= cur_oper_word;
						break;
					case nary_action_t::N_DIFF:
						output_word = cur_oper_word;
						cur_word_action = nary_action_t::N_CDIFF;
						break;
					case nary_action_t::N_CDIFF:
						output_word &= ~cur_oper_word;
						break;
					case nary_action_t::N_SYMDIFF:
					case nary_action_t::N_CSYMDIFF:
						output_word ^= cur_oper_word;
						break;
					case nary_action_t::N_INFEAS:
					case nary_action_t::N_DELETE:
						abort(); break;
					}
				}

				output_semiword_iter.set(output_word);
				output_semiword_iter.next();
			}

			action_queue.pop();
			operset_queue.pop();
		}

		for (auto semiword_it_it = semiword_iters.begin(); semiword_it_it != semiword_iters.end(); semiword_it_it++)
			assert(!semiword_it_it->has_top());
	}

	// Ensure we end in a consistent state
	assert(!action_queue.size() && !operset_queue.size());

	// Finally, compact the output CBLQ if requested
	if (this->conf.compact_after_setop)
		output->compact();

	return output;
}

// Explicit instantiation of 2D-4D CBLQ N-ary set operations
template class CBLQSetOperationsNAry2Dense<2>;
template class CBLQSetOperationsNAry2Dense<3>;
template class CBLQSetOperationsNAry2Dense<4>;
