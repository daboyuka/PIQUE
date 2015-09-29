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
 * cblq-traversal-impl.hpp
 *
 *  Created on: Jul 31, 2014
 *      Author: David A. Boyuka II
 */
#ifndef CBLQ_TRAVERSAL_IMPL_HPP_
#define CBLQ_TRAVERSAL_IMPL_HPP_

#include <cstdint>
#include <cassert>

#include "pique/region/cblq/cblq-traversal.hpp"

// LevelStateGenFn signature:
//   (int levels, int level, uint64_t level_len) -> LevelStateT
// ProcessCBLQCodeFn signature:
//   (char code, int code_pos, int level, LevelStateT level_state, QueueElemT queue_elem, queue_iter_t &queue_out_it) -> voidtemplate<int ndim>
template<int ndim, typename QueueElemT>
template<typename LevelStateT, typename LevelStateGenFn, typename ProcessCBLQCodeFn>
void CBLQTraversal<ndim, QueueElemT>::traverse_bf(const CBLQRegionEncoding<ndim> &cblq, LevelStateGenFn level_state_gen, QueueElemT first_queue_elem, ProcessCBLQCodeFn process_code_fn) {
	// Construct an inner traversal loop execute lambda
	auto exec_inner_loop_fn = [&cblq, level_state_gen, process_code_fn](int levels, int level, uint64_t level_len, InnerLoopParameters inner_loop_params) -> void {
		LevelStateT level_state = level_state_gen(levels, level, level_len); // Generate the level state

		// Build a wrapper for process_code_fn that provides the additional level state required
		auto level_process_code_fn = [process_code_fn, level, &level_state](char code, int code_pos, QueueElemT queue_elem, queue_iter_t &queue_out_it) -> void {
			process_code_fn(code, code_pos, level, level_state, queue_elem, queue_out_it);
		};

		// Invoke the inner traversal loop
		traverse_bf_inner_loop(cblq, level_process_code_fn, inner_loop_params);
	};

	// Delegate traversal to the outer/inner traversal loop algorithm
	traverse_bf_outer_loop(cblq, first_queue_elem, exec_inner_loop_fn);
}

// ExecuteInnerLoopFn signature:
//   (int levels, int level, uint64_t level_len, InnerLoopParameters inner_loop_params) -> LevelProcessCBLQCodeFn
// Note: ExecuteInnerLoopFn should call traverse_bf_inner_loop exactly once, passing along inner_loop_params
template<int ndim, typename QueueElemT>
template<typename ExecuteInnerLoopFn>
void CBLQTraversal<ndim, QueueElemT>::traverse_bf_outer_loop(const CBLQRegionEncoding<ndim> &cblq, QueueElemT first_queue_elem, ExecuteInnerLoopFn exec_inner_loop_fn) {
	using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;
	using cblq_semiword_t = typename CBLQRegionEncoding<ndim>::cblq_semiword_t;
	std::vector< QueueElemT > this_level_queue(1, first_queue_elem);
	std::vector< QueueElemT > next_level_queue;

	const int levels = cblq.get_num_levels();
	const bool has_dense_suffix = cblq.has_dense_suffix;
	const int non_dense_levels = has_dense_suffix ? levels - 1 : levels;
	const uint64_t last_level_len = (has_dense_suffix ? cblq.dense_suffix->get_num_semiwords() : cblq.level_lens[levels - 1]);

	auto cblq_word_it = cblq.words.cbegin();

	for (int level = 0; level < non_dense_levels; level++) {
		const uint64_t level_len = cblq.level_lens[level];
		const uint64_t next_level_len = (level + 1 == levels - 1 ? last_level_len : level + 1 == levels ? 0 : cblq.level_lens[level + 1]);

		next_level_queue.resize(next_level_len);
		auto queue_it = this_level_queue.cbegin();
		auto queue_end_it = this_level_queue.cend();
		auto next_queue_it = next_level_queue.begin();
		auto next_queue_end_it = next_level_queue.cend();

		exec_inner_loop_fn(levels, level, level_len, InnerLoopParameters(queue_it, queue_end_it, next_queue_it, next_queue_end_it, cblq_word_it));

		this_level_queue = std::move(next_level_queue);
		next_level_queue.clear();
	}

	assert(cblq_word_it == cblq.words.cend());

	if (cblq.has_dense_suffix) {
		// TODO: Optimize this more
		const uint64_t level = non_dense_levels;
		const uint64_t level_len = cblq.dense_suffix->get_num_semiwords();

		auto dense_word_it = const_cast< CBLQRegionEncoding<ndim> & >(cblq).dense_suffix->begin(); // constness hack
		auto queue_it = this_level_queue.cbegin();
		auto queue_end_it = this_level_queue.cend();
		auto next_queue_it = next_level_queue.begin(); // Should never be accessed
		auto next_queue_end_it = next_queue_it;

		exec_inner_loop_fn(levels, level, level_len, InnerLoopParameters(queue_it, queue_end_it, next_queue_it, next_queue_end_it, dense_word_it));
	}
}

// LevelProcessCBLQCodeFn signature:
//   (char code, int code_pos, QueueElemT queue_elem, queue_iter_t &queue_out_it) -> void
template<int ndim, typename QueueElemT>
template<typename LevelProcessCBLQCodeFn>
void CBLQTraversal<ndim, QueueElemT>::traverse_bf_inner_loop(LevelProcessCBLQCodeFn level_process_code_fn, InnerLoopParameters inner_loop_params) {
	queue_citer_t queue_it = inner_loop_params.queue_it;
	queue_citer_t queue_end_it = inner_loop_params.queue_end_it;
	queue_iter_t queue_out_it = inner_loop_params.queue_out_it;
	queue_citer_t queue_out_end_it = inner_loop_params.queue_out_end_it;

	if (inner_loop_params.cblq_word_it) {
		cblq_word_citer_t cblq_word_it = *inner_loop_params.cblq_word_it;

		for (; queue_it != queue_end_it; ++queue_it, ++cblq_word_it) {
			const cblq_word_t cblq_word = *cblq_word_it;
			const QueueElemT &queue_elem = *queue_it;

			for (int i = 0; i < CBLQRegionEncoding<ndim>::CODES_PER_WORD; i++) {
				const char code = (cblq_word >> (i * CBLQRegionEncoding<ndim>::BITS_PER_CODE)) & 0b11;
				level_process_code_fn(code, i, queue_elem, queue_out_it);
			}
		}

		// Update with the final cblq word iterator
		*inner_loop_params.cblq_word_it = cblq_word_it;
	} else {
		cblq_semiword_iter_t dense_word_it = *inner_loop_params.cblq_semiword_it;

		for (; queue_it != queue_end_it; ++queue_it, dense_word_it.next()) {
			cblq_semiword_t semiword = dense_word_it.top();
			const QueueElemT &queue_elem = *queue_it;

			for (int i = 0; i < CBLQRegionEncoding<ndim>::CODES_PER_WORD; i++) {
				char code = (semiword >> i) & 0b1;
				level_process_code_fn(code, i, queue_elem, queue_out_it);
			}
		}

		assert(!dense_word_it.has_top());
	}

	assert(queue_out_it == queue_out_end_it);
}



// ProcessBlockFn signature:
//   (block_size_t block_size, block_offest_t block_offset) -> void
template<int ndim>
template<bool visit_present_blocks, typename ProcessBlockFn>
void CBLQTraversalBlocks<ndim>::traverse_bf(const CBLQRegionEncoding<ndim> &cblq, ProcessBlockFn process_block_fn) {
	// Construct a trivial execute inner block traversal loop function that always returns the same block process function, and that maintains no level state
	auto exec_inner_block_loop_fn = [process_block_fn](int levels, int level, uint64_t level_len, block_size_t level_block_size, InnerBlockLoopParameters inner_block_loop_params) -> void {
		// Always the same block process function, and no level state needed (besides block_size, which is handled by the CBLQTraversalBlocks framework)
		CBLQTraversalBlocks<ndim>::template traverse_bf_inner_loop<visit_present_blocks>(process_block_fn, inner_block_loop_params);
	};

	CBLQTraversalBlocks<ndim>::template traverse_bf_outer_loop(cblq, exec_inner_block_loop_fn);
}

// ExecuteInnerBlockLoopFn signature:
//   (int levels, int level, uint64_t level_len, block_size_t level_block_size, InnerBlockLoopParameters inner_block_loop_params) -> void
// Note: ExecuteInnerBlockLoopFn should call traverse_bf_inner_loop exactly once, passing along inner_block_loop_params
template<int ndim>
template<typename ExecuteInnerBlockLoopFn>
void CBLQTraversalBlocks<ndim>::traverse_bf_outer_loop(const CBLQRegionEncoding<ndim> &cblq, ExecuteInnerBlockLoopFn exec_inner_block_loop_fn) {
	// Wrap the executeinner traversal loop function, providing the level's block size
	auto exec_inner_loop_fn = [exec_inner_block_loop_fn](int levels, int level, uint64_t level_len, BaseInnerLoopParameters inner_loop_params) -> void {
		const block_size_t level_block_size = (1ULL << (levels - level - 1) * ndim);

		InnerBlockLoopParameters inner_block_loop_params(inner_loop_params, level_block_size);

		exec_inner_block_loop_fn(levels, level, level_len, level_block_size, inner_block_loop_params);
	};

	CBLQTraversal<ndim, block_offset_t>::template traverse_bf_outer_loop(cblq, (block_offset_t)0, exec_inner_loop_fn);
}

// ProcessBlockFn signature:
//   (block_size_t block_size, block_offest_t block_offset) -> void
template<int ndim>
template<bool visit_present_blocks, typename ProcessBlockFn>
void CBLQTraversalBlocks<ndim>::traverse_bf_inner_loop(ProcessBlockFn process_block_fn, InnerBlockLoopParameters inner_block_loop_params) {
	const block_size_t level_block_size = inner_block_loop_params.level_block_size;
	constexpr char BLOCK_CODE_TO_VISIT = (visit_present_blocks ? 0b01 : 0b00);

	auto level_process_code_fn = [process_block_fn, level_block_size](char code, int code_pos, block_offset_t block_offset, queue_iter_t &queue_out_it) -> void {
		if (code == BLOCK_CODE_TO_VISIT) {
			process_block_fn(level_block_size, block_offset + code_pos * level_block_size);
		} else if (code == 0b10) {
			*queue_out_it = block_offset + code_pos * level_block_size;
			++queue_out_it;
		}
	};

	CBLQTraversal<ndim, block_offset_t>::template traverse_bf_inner_loop(level_process_code_fn, inner_block_loop_params.base_inner_loop_params);
}

#endif /* CBLQ_TRAVERSAL_IMPL_HPP_ */
