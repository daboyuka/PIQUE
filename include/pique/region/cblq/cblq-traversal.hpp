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
 * cblq-traversal.hpp
 *
 *  Created on: Jul 31, 2014
 *      Author: David A. Boyuka II
 */
#ifndef CBLQ_TRAVERSAL_HPP_
#define CBLQ_TRAVERSAL_HPP_

#include <boost/optional.hpp>

#include <pique/region/cblq/cblq.hpp>

template<int ndim, typename QueueElemT>
class CBLQTraversal {
public:
	using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;
	using cblq_semiword_t = typename CBLQRegionEncoding<ndim>::cblq_semiword_t;
	typedef typename std::vector< QueueElemT >::iterator queue_iter_t;
	typedef typename std::vector< QueueElemT >::const_iterator queue_citer_t;

	using cblq_word_citer_t = typename std::vector< cblq_word_t >::const_iterator;
	using cblq_semiword_iter_t = typename CBLQSemiwords<ndim>::iterator;

	// An opaque structure for carrying the traversal state between traverse_bf_outer_loop and traverse_bf_inner_loop via the user-supplied ExecuteInnerLoopFn
	class InnerLoopParameters {
	private:
		InnerLoopParameters(queue_citer_t queue_it, queue_citer_t queue_end_it, queue_iter_t queue_out_it, queue_citer_t queue_out_end_it, cblq_word_citer_t &cblq_word_it) :
			queue_it(queue_it), queue_end_it(queue_end_it), queue_out_it(queue_out_it), queue_out_end_it(queue_out_end_it),
			cblq_word_it(cblq_word_it), cblq_semiword_it()
		{}

		InnerLoopParameters(queue_citer_t queue_it, queue_citer_t queue_end_it, queue_iter_t queue_out_it, queue_citer_t queue_out_end_it, cblq_semiword_iter_t cblq_semiword_it) :
			queue_it(queue_it), queue_end_it(queue_end_it), queue_out_it(queue_out_it), queue_out_end_it(queue_out_end_it),
			cblq_word_it(), cblq_semiword_it(cblq_semiword_it)
		{}

		const queue_citer_t queue_it;
		const queue_citer_t queue_end_it;
		const queue_iter_t queue_out_it;
		const queue_citer_t queue_out_end_it;

		boost::optional< cblq_word_citer_t& > cblq_word_it;
		boost::optional< cblq_semiword_iter_t > cblq_semiword_it;

		template<int ndim2, typename QueueElemT2> friend class CBLQTraversal;
	};

	// LevelStateGenFn signature:
	//   (int levels, int level, uint64_t level_len) -> LevelStateT
	// ProcessCBLQCodeFn signature:
	//   (char code, int code_pos, int level, LevelStateT level_state, QueueElemT queue_elem, queue_iter_t &queue_out_it) -> void
	template<typename LevelStateT, typename LevelStateGenFn, typename ProcessCBLQCodeFn>
	static void traverse_bf(const CBLQRegionEncoding<ndim> &cblq, LevelStateGenFn level_state_gen, QueueElemT first_queue_elem, ProcessCBLQCodeFn process_code_fn);

	// ExecuteInnerLoopFn signature:
	//   (int levels, int level, uint64_t level_len, InnerLoopParameters inner_loop_params) -> LevelProcessCBLQCodeFn
	// Note: ExecuteInnerLoopFn should call traverse_bf_inner_loop exactly once, passing along inner_loop_params
	template<typename ExecuteInnerLoopFn>
	static void traverse_bf_outer_loop(const CBLQRegionEncoding<ndim> &cblq, QueueElemT first_queue_elem, ExecuteInnerLoopFn exec_inner_loop_fn);

	// LevelProcessCBLQCodeFn signature:
	//   (char code, int code_pos, QueueElemT queue_elem, queue_iter_t &queue_out_it) -> void
	template<typename LevelProcessCBLQCodeFn>
	static void traverse_bf_inner_loop(LevelProcessCBLQCodeFn level_process_code_fn, InnerLoopParameters inner_loop_params);
};

class CBLQNodeBlockTypes {
public:
	typedef uint64_t block_size_t;
	typedef uint64_t block_offset_t;
};

template<int ndim>
class CBLQTraversalBlocks {
public:
	using block_size_t = CBLQNodeBlockTypes::block_size_t;
	using block_offset_t = CBLQNodeBlockTypes::block_offset_t;

	using BaseInnerLoopParameters = typename CBLQTraversal<ndim, block_offset_t>::InnerLoopParameters;
	using queue_iter_t = typename CBLQTraversal<ndim, block_offset_t>::queue_iter_t;

	class InnerBlockLoopParameters {
	private:
		InnerBlockLoopParameters(BaseInnerLoopParameters &base_inner_loop_params, block_size_t level_block_size) :
			base_inner_loop_params(base_inner_loop_params),
			level_block_size(level_block_size)
		{}

		BaseInnerLoopParameters &base_inner_loop_params;
		const block_size_t level_block_size;

		template<int ndim2> friend class CBLQTraversalBlocks;
	};

	// ProcessBlockFn signature:
	//   (block_size_t block_size, block_offest_t block_offset) -> void
	template<bool visit_present_blocks, typename ProcessBlockFn>
	static void traverse_bf(const CBLQRegionEncoding<ndim> &cblq, ProcessBlockFn process_block_fn);

	// ExecuteInnerBlockLoopFn signature:
	//   (int levels, int level, uint64_t level_len, block_size_t level_block_size, InnerBlockLoopParameters inner_block_loop_params) -> void
	// Note: ExecuteInnerBlockLoopFn should call traverse_bf_inner_loop exactly once, passing along inner_block_loop_params
	template<typename ExecuteInnerBlockLoopFn>
	static void traverse_bf_outer_loop(const CBLQRegionEncoding<ndim> &cblq, ExecuteInnerBlockLoopFn exec_inner_block_loop_fn);

	// ProcessBlockFn signature:
	//   (block_size_t block_size, block_offest_t block_offset) -> void
	template<bool visit_present_blocks, typename ProcessBlockFn>
	static void traverse_bf_inner_loop(ProcessBlockFn process_block_fn, InnerBlockLoopParameters inner_block_loop_params);

	// ProcessBlockFn signature:
	//   (block_size_t block_size, block_offest_t block_offset) -> void
	template<bool visit_present_blocks, typename ProcessBlockFn>
	static void traverse_df(const CBLQRegionEncoding<ndim> &cblq, ProcessBlockFn process_block_fn);

	// ProcessBlockFn/ProcessLeafBlockFn signature:
	//   (block_size_t block_size, block_offest_t block_offset) -> void
	template<bool visit_present_blocks, typename ProcessBlockFn, typename ProcessLeafBlockFn>
	static void traverse_df_leafopt(const CBLQRegionEncoding<ndim> &cblq, ProcessBlockFn process_block_fn, ProcessLeafBlockFn process_leaf_block_fn);

	// LevelProcessBlockFns: a class with members:
	//   <some type> get_level_process_block_fn<level_from_bottom>(): return a functor with the signature for LevelProcessBlockFn
	// LevelProcessBlockFn signature:
	//   (block_size_t block_size, block_offest_t block_offset) -> void
	template<bool visit_present_blocks, typename LevelProcessBlockFns>
	static void traverse_df_levelopt(const CBLQRegionEncoding<ndim> &cblq, LevelProcessBlockFns level_process_block_fns);
};

#include "pique/region/cblq/impl/cblq-traversal-impl.hpp"
#include "pique/region/cblq/impl/cblq-traversal-df-impl.hpp"

#endif /* CBLQ_TRAVERSAL_HPP_ */
