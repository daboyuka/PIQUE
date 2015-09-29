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

extern "C" {
#include "setops/cblq/cblq-setops-actiontables.h"
}

template<int ndim>
static inline void unpack_cblq_word(typename CBLQRegionEncoding<ndim>::cblq_word_t word, unsigned char *codes) {
	for (int code_pos = 0; code_pos < CBLQRegionEncoding<ndim>::CODES_PER_WORD; code_pos++) {
		*codes++ = (word & 0b11);
		word >>= CBLQRegionEncoding<ndim>::BITS_PER_CODE;
	}
}

template<int ndim>
static inline typename CBLQRegionEncoding<ndim>::cblq_word_t repack_cblq_word(const unsigned char *codes) {
	typename CBLQRegionEncoding<ndim>::cblq_word_t word = 0;
	codes += CBLQRegionEncoding<ndim>::CODES_PER_WORD;
	for (int code_pos = 0; code_pos < CBLQRegionEncoding<ndim>::CODES_PER_WORD; code_pos++) {
		word <<= CBLQRegionEncoding<ndim>::BITS_PER_CODE;
		word |= *--codes; // Assume codes have no 1's outside the first 2 bits, for efficiency
	}
	return word;
}

template<int ndim>
static inline void append_action_as_action_word(std::vector< nary_action_compact_t > &actions, nary_action_compact_t action) {
	actions.insert(actions.end(), CBLQRegionEncoding<ndim>::CODES_PER_WORD, action);
}

// Takes the action word queue and the outinds_present bitmask, and remaps outinds to a dense mapping, with outinds
// pointing to DELETE actions mapped to -1, and missing outinds removed from the outind space
template<int ndim>
static void dense_remap_outinds(const std::vector< nary_action_compact_t > &actions, boost::dynamic_bitset<> &outinds_present, std::vector< std::vector<size_t> > &outinds, size_t &outind_ubound) {
	// First, compute the compaction mapping for the outinds (remove all indices pointing to NO-OP and DELETE actions)
	const size_t max_unique_outids = actions.size(); // Max one action per action
	std::vector<size_t> outind_mapping(max_unique_outids + 1, -1); // outind_mapping[outind + 1] -> mapped_outind (we do +1 so that outind == -1 will work)
	//outind_mapping.resize();

	size_t mapped_outind = 0;
	size_t cur_outind = 0;
	for (auto action_it = actions.cbegin(); action_it != actions.cend(); action_it++, cur_outind++) {
		if (!outinds_present[cur_outind]) { // No output at this outind
			//outind_mapping.push_back(-1); // Not needed, no outind will look up this index
		} else if (*action_it == N_DELETE) { // Delete action at this outind
			//outind_mapping[cur_outind + 1] = -1;
			outinds_present[cur_outind] = false;
		} else {
			outind_mapping[cur_outind + 1] = mapped_outind++;
		}
	}

	outind_ubound = mapped_outind; // Exclusive upper bound on all mapped outinds

	// Post-condition: mapped_outind[x] = y such that:
	//   If there does not exists any output_level_outinds[i][j] == x, or the x'th action state is N_DELETE, then y == -1
	//   Otherwise, y >= 0, and all such y are densely packed (i.e., given x->y, the largest x' < x with x'->y' and y' >=0 has y' = y-1, unless y == 0, in which case no such x' exists)
	// mapped_outind[] forms a dense mapping of outinds, which can be used to compact that array

	// Densely remap all outinds produced
	for (auto cblq_outinds_it = outinds.begin(); cblq_outinds_it != outinds.end(); cblq_outinds_it++) {
		for (auto outind_it = cblq_outinds_it->begin(); outind_it != cblq_outinds_it->end(); outind_it++) {
			*outind_it = outind_mapping[*outind_it + 1];
		}
	}
}

// Takes the outinds_present bitmask, and remaps all outind-referenced actions in the given action queue into expanded action words
template<int ndim>
static void initialize_level_state(
		const boost::dynamic_bitset<> &outinds_present,
		const size_t outind_ubound,
		std::vector< nary_action_compact_t > &state_actions,
		std::vector< typename CBLQRegionEncoding<ndim>::cblq_word_t > &state_cblq_words)
{
	typedef typename CBLQRegionEncoding<ndim>::cblq_word_t cblq_word_t;

	// Allocate a new action-words queue
	std::vector<nary_action_compact_t> new_actions;
	new_actions.reserve(outind_ubound);

	// Clear the initial CBLQ word state vector
	state_cblq_words.clear();

	// Iterate over all outind-referenced actions, appending them as expanded action words
	//for (size_t action_outind = outinds_present.find_first(); action_outind != boost::dynamic_bitset<>::npos; action_outind = outinds_present.find_next(action_outind)) {
	for (size_t action_outind = 0; action_outind < state_actions.size(); action_outind++) {
		if (!outinds_present[action_outind])
			continue;

		const nary_action_compact_t action = state_actions[action_outind];
		append_action_as_action_word<ndim>(new_actions, action);
		state_cblq_words.push_back(NARY_ACTION_INITIAL_CODE[action] ?
										CBLQRegionEncoding<ndim>::ONE_CODES_WORD :
										CBLQRegionEncoding<ndim>::ZERO_CODES_WORD);
	}

	// Overwrite the current action-word queue with the newly-constructed one
	state_actions = std::move(new_actions);
}

// Takes the outinds_present bitmask, and remaps all outind-referenced actions in the given action queue into expanded action words
template<int ndim>
static void initialize_dense_level_state(
		const boost::dynamic_bitset<> &outinds_present,
		const size_t outind_ubound,
		const std::vector< nary_action_compact_t > &state_actions,
		std::vector< nary_action_compact_t > &out_actions,
		CBLQSemiwords<ndim> &initial_semiwords)
{
	typedef typename CBLQRegionEncoding<ndim>::cblq_semiword_t cblq_semiword_t;

	// Allocate the output action queue and semiword buffer
	out_actions.clear();
	out_actions.reserve(outind_ubound);
	initial_semiwords.expand(outind_ubound);

	auto semiword_it = initial_semiwords.begin();
	// Iterate over all outind-referenced actions, appending them as expanded action words
	for (size_t action_outind = outinds_present.find_first(); action_outind != boost::dynamic_bitset<>::npos; action_outind = outinds_present.find_next(action_outind)) {
		const nary_action_compact_t action = state_actions[action_outind];

		out_actions.push_back(action);
		semiword_it.set(NARY_ACTION_INITIAL_CODE[action] ?
							CBLQRegionEncoding<ndim>::FULL_SEMIWORD :
							CBLQRegionEncoding<ndim>::EMPTY_SEMIWORD);
		semiword_it.next();
	}
}


template<int ndim>
boost::shared_ptr< CBLQRegionEncoding<ndim> >
CBLQSetOperationsNAry3Dense<ndim>::nary_set_op_impl(RegionEncodingCPtrCIter cblq_it, RegionEncodingCPtrCIter cblq_end_it, NArySetOperation op) const {
	typedef std::array<nary_action_compact_t, CBLQRegionEncoding<ndim>::CODES_PER_WORD> action_word_t;

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

	// --- Level processing round ---
	// Read-write: action states
	std::vector<nary_action_compact_t> level_actions;
	append_action_as_action_word<ndim>(level_actions, first_action);
	// Read-write: CBLQ word states
	std::vector<cblq_word_t> level_cblq_words;
	level_cblq_words.push_back(NARY_ACTION_INITIAL_CODE[first_action] ? CBLQRegionEncoding<ndim>::ONE_CODES_WORD : CBLQRegionEncoding<ndim>::ZERO_CODES_WORD);
	// Read-only: index array for each CBLQ for each enqueued action (i.e., private action queue via indirection)
	std::vector< std::vector<size_t> > level_outinds(num_operands, std::vector<size_t>(1, 0)); // Fill with vectors, each with one element: 0

	// -- Level processing round output ---
	// Write-only: output index array for each CBLQ (i.e., private action queue via indirection)
	std::vector< std::vector<size_t> > output_level_outinds(num_operands);
	// Write-only: output referenced outinds
	boost::dynamic_bitset<> output_level_outinds_present(1 * CBLQRegionEncoding<ndim>::CODES_PER_WORD, false); // outind upper bound: < (actions * CODES_PER_WORD)
	// Write-only: exclusive upper bound on all outinds (after remapping)
	size_t output_level_outind_ubound = 1;

	/*
	auto dump_state_fn = [&]() -> void {
		printf("\n--- N-ary3 state dump: ---\n");
		printf("level_action_words/level_cblq_words(size = %llu):", level_actions.size());
		for (size_t i = 0; i < level_actions.size(); i += CBLQRegionEncoding<ndim>::CODES_PER_WORD) {
			if (i % 8 == 0)
				printf("\n");
			else
				printf(" ");

			for (int cp = 0; cp < CBLQRegionEncoding<ndim>::CODES_PER_WORD; cp++)
				putchar(NARY_ACTION_CHAR_CODE[level_actions[i + cp]]);
			printf("/");
			CBLQRegionEncoding<ndim>::print_cblq_word(level_cblq_words[i]);
		}
		printf("\nlevel_outinds:");
		for (size_t i = 0; i < num_operands; i++) {
			printf("\nOperand %llu:", i);
			for (size_t j = 0; j < level_outinds[i].size(); j++) {
				if (j % 8 == 0)
					printf("\n");
				else
					printf(" ");
				printf("%llu", level_outinds[i][j]);
			}
		}
		printf("\noutput_level_outinds:");
		for (size_t i = 0; i < num_operands; i++) {
			printf("\nOperand %llu:", i);
			for (size_t j = 0; j < output_level_outinds[i].size(); j++) {
				if (j % 8 == 0)
					printf("\n");
				else
					printf(" ");
				printf("%llu", output_level_outinds[i][j]);
			}
		}
		printf("\noutput_level_outinds_present: ");
		std::cout << output_level_outinds_present << std::endl;
		printf("\n--------------------------\n");
	};
	*/

	unsigned char oper_cblq_codes[CBLQRegionEncoding<ndim>::CODES_PER_WORD];
	unsigned char state_cblq_codes[CBLQRegionEncoding<ndim>::CODES_PER_WORD];
	// For each CBLQ level
	for (int level = 0; level < non_dense_levels; level++) {
		output->level_lens[level] = output_level_outind_ubound; // The number of words we will output this level is already known: it's the number of non-delete actions that are queued

		//printf("Level %d start state:\n", level);
		//dump_state_fn();

		// For each operand
		for (size_t operand = 0; operand < num_operands; operand++) {
			auto &cblq_word_it = cblq_word_its[operand]; // Get the CBLQ word iterator for this CBLQ
			auto &outinds = level_outinds[operand];
			auto &output_outinds = output_level_outinds[operand];

			// For each action in which this operand participates
			for (auto outind_it = outinds.cbegin(); outind_it != outinds.cend(); outind_it++) {
				const size_t outind = *outind_it;
				const cblq_word_t oper_cblq_word = *cblq_word_it++;

				unpack_cblq_word<ndim>(oper_cblq_word, oper_cblq_codes);

				// If this is a DELETE action, enqueue DELETEs for all children, then skip any further processing
				if (outind == -1) {
					for (int code_pos = 0; code_pos < CBLQRegionEncoding<ndim>::CODES_PER_WORD; code_pos++)
						if (oper_cblq_codes[code_pos] == 0b10)
							output_outinds.push_back(-1);
					continue;
				}

				cblq_word_t &state_cblq_word = level_cblq_words[outind];
				nary_action_compact_t *state_action_word = &level_actions[outind * CBLQRegionEncoding<ndim>::CODES_PER_WORD];

				unpack_cblq_word<ndim>(state_cblq_word, state_cblq_codes);

				// For each code in the current CBLQ word
				for (int code_pos = 0; code_pos < CBLQRegionEncoding<ndim>::CODES_PER_WORD; code_pos++) {
					const cblq_word_t oper_cblq_code = oper_cblq_codes[code_pos];

					nary_action_compact_t &state_action_code = state_action_word[code_pos];
					unsigned char &state_cblq_code = state_cblq_codes[code_pos];

					// (action state, code state, operand code) -> (new action state, new code state)
					const nary_action_table_entry_t &transition = NARY_ACTION_TABLE[state_action_code][state_cblq_code][oper_cblq_code];

					// Update action/code state
					state_action_code = transition.action;
					state_cblq_code = (unsigned char)transition.output;

					// If applying the action to the current code requires future work to be enqueued (exactly when
					// the current code is a two-code), do so now
					if (oper_cblq_code == 0b10) {
						const size_t out_outind = (outind * CBLQRegionEncoding<ndim>::CODES_PER_WORD) + code_pos;
						output_outinds.push_back(out_outind);
						output_level_outinds_present.set(out_outind, true);
					}
				}

				state_cblq_word = repack_cblq_word<ndim>(state_cblq_codes);
			}
		}

		//printf("Level %d end state:\n", level);
		//dump_state_fn();

		// - Append CBLQ word state to the output CBLQ
		output_words.insert(output_words.end(), level_cblq_words.begin(), level_cblq_words.end());
		level_cblq_words.clear();

		// If there is still another iteration, initialize the level state for the next level
		if (level != non_dense_levels - 1) {
			// - Update initial state for the next level based on output of this level
			// Densely remap all outinds produced in the last level
			dense_remap_outinds<ndim>(level_actions, output_level_outinds_present, output_level_outinds, output_level_outind_ubound);

			// update level_state_action_words, level_state_cblq_words
			// Densely copy and word-expand all actions that should be enqueued (due to being referenced with an outind)
			initialize_level_state<ndim>(output_level_outinds_present, output_level_outind_ubound, level_actions, level_cblq_words);

			// update level_outinds
			level_outinds.swap(output_level_outinds); // output_level_outinds-content -> level_outinds, and level_outinds-memory -> output_level_outinds so the memory can be reused

			// Clear output vectors, etc.
			for (auto operand_outinds_it = output_level_outinds.begin(); operand_outinds_it != output_level_outinds.end(); operand_outinds_it++)
				operand_outinds_it->clear();
			output_level_outinds_present.reset();
			output_level_outinds_present.resize(output_level_outind_ubound * CBLQRegionEncoding<ndim>::CODES_PER_WORD, false); // This is necessary, however, since any bit may be set
		}
	}

	if (has_dense_suffix) {
		// Set the (non-dense) leaf level length to 0
		output->level_lens[levels - 1] = 0;

		std::vector< typename CBLQSemiwords<ndim>::iterator > semiword_iters;
		for (auto it = cblq_it; it != cblq_end_it; it++) {
			CBLQRegionEncoding<ndim> &cblq = const_cast<CBLQRegionEncoding<ndim> &>(**it); // const-ness hack
			semiword_iters.push_back(cblq.dense_suffix->begin());
		}

		CBLQSemiwords<ndim> &output_semiwords = *output->dense_suffix;
		std::vector<nary_action_compact_t> dense_action_queue;

		// If there is a dense suffix, the first task is to initialize the level state for this leaf level,
		// since it was not done at the end of the previous loop (since this initialization is a bit different for a dense suffix)
		dense_remap_outinds<ndim>(level_actions, output_level_outinds_present, output_level_outinds, output_level_outind_ubound);

		// Initialize the special dense suffix state
		initialize_dense_level_state(output_level_outinds_present, output_level_outind_ubound, level_actions, dense_action_queue, output_semiwords);

		// Skip this and just use output_level_outinds directly, since we don't need to write to output_level_outinds in this pass
		// // update level_outinds
		// level_outinds.swap(output_level_outinds); // output_level_outinds-content -> level_outinds, and level_outinds-memory -> output_level_outinds so the memory can be reused

		// Execute the main loop
		// For each operand
		for (size_t operand = 0; operand < num_operands; operand++) {
			auto &semiword_it = semiword_iters[operand]; // Get the CBLQ word iterator for this CBLQ
			auto &outinds = output_level_outinds[operand];

			// For each action in which this operand participates
			for (auto outind_it = outinds.cbegin(); outind_it != outinds.cend(); outind_it++) {
				const size_t outind = *outind_it;
				const cblq_semiword_t oper_semiword = semiword_it.top();
				semiword_it.next();

				if (outind == -1)
					continue;

				nary_action_compact_t &action = dense_action_queue[outind];
				cblq_semiword_t state_semiword = output_semiwords.get(outind);

				switch (action) {
				case nary_action_t::N_UNION:
					state_semiword |= oper_semiword;
					break;
				case nary_action_t::N_INTER:
					state_semiword &= oper_semiword;
					break;
				case nary_action_t::N_DIFF:
					state_semiword = oper_semiword;
					action = nary_action_t::N_CDIFF;
					break;
				case nary_action_t::N_CDIFF:
					state_semiword &= ~oper_semiword;
					break;
				case nary_action_t::N_SYMDIFF:
				case nary_action_t::N_CSYMDIFF:
					state_semiword ^= oper_semiword;
					break;
				}

				output_semiwords.set(outind, state_semiword);
			}
		}

		for (auto semiword_it_it = semiword_iters.begin(); semiword_it_it != semiword_iters.end(); semiword_it_it++)
			assert(!semiword_it_it->has_top());
	} else {
		// We can only check this if there wasn't a dense suffix

		// Ensure no additional actions were enqueued
		for (auto operand_outinds_it = output_level_outinds.begin(); operand_outinds_it != output_level_outinds.end(); operand_outinds_it++)
			assert(operand_outinds_it->empty());
	}

	// Finally, compact the output CBLQ if requested
	if (this->conf.compact_after_setop)
		output->compact();

	return output;
}

// Explicit instantiation of 2D-4D CBLQ N-ary set operations
template class CBLQSetOperationsNAry3Dense<2>;
template class CBLQSetOperationsNAry3Dense<3>;
template class CBLQSetOperationsNAry3Dense<4>;
