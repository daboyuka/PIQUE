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
 * cblq-tranversal-df-impl.hpp
 *
 *  Created on: Aug 1, 2014
 *      Author: David A. Boyuka II
 */
#ifndef CBLQ_TRANVERSAL_DF_IMPL_HPP_
#define CBLQ_TRANVERSAL_DF_IMPL_HPP_

#include <vector>

#include "pique/region/cblq/cblq.hpp"

template<int ndim>
class CBLQDepthFirstTraversalAccess {
public:
	using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;
	using cblq_word_citer_t = typename std::vector< cblq_word_t >::const_iterator;
	using cblq_semiword_bitblock_t = typename CBLQSemiwords<ndim>::block_t;
	using cblq_semiword_bitblock_citer_t = typename std::vector< cblq_semiword_bitblock_t >::const_iterator;
	static constexpr int SEMIWORDS_PER_BITBLOCK = CBLQSemiwords<ndim>::SEMIWORDS_PER_BLOCK;

	static const std::vector< cblq_word_t > SINGLE_CHILD;
	static void get_level_iter_bounds(
			const CBLQRegionEncoding<ndim> &cblq,
			int level_from_bottom,
			cblq_word_citer_t &it, cblq_word_citer_t &end_it)
	{
		if (level_from_bottom >= cblq.level_lens.size()) {
			it = SINGLE_CHILD.cbegin();
			end_it = SINGLE_CHILD.cend();
			return;
		}

		uint64_t sum = 0;
		const int level = cblq.level_lens.size() - level_from_bottom - 1;
		for (int i = 0; i < level; i++)
			sum += cblq.level_lens[i];

		it = cblq.words.cbegin() + sum;
		end_it = it + cblq.level_lens[level];
	}

	static typename CBLQSemiwords<ndim>::iterator get_level_dense_iter_bounds(const CBLQRegionEncoding<ndim> &cblq)
	{
		return const_cast< CBLQRegionEncoding<ndim>& >(cblq).dense_suffix->begin();
	}

	static void get_level_dense_iter_bounds(const CBLQRegionEncoding<ndim> &cblq, uint64_t &num_semiwords, cblq_semiword_bitblock_citer_t &it)
	{
		it = cblq.dense_suffix->semiwords.cbegin();
		num_semiwords = cblq.dense_suffix->num_semiwords;
	}

	static bool has_dense_suffix(const CBLQRegionEncoding<ndim> &cblq) { return cblq.has_dense_suffix; }
};

template<int ndim>
const std::vector< typename CBLQRegionEncoding<ndim>::cblq_word_t >
	CBLQDepthFirstTraversalAccess<ndim>::SINGLE_CHILD(1, 0b10);

template<int ndim, int level_from_bottom, bool visit_present_blocks, typename LevelProcessBlockFns>
class CBLQDepthFirstTraversalConstants {
public:
	using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;
	using cblq_word_citer_t = typename std::vector< cblq_word_t >::const_iterator;

	using cblq_semiword_t = typename CBLQRegionEncoding<ndim>::cblq_semiword_t;
	using cblq_semiword_bitblock_t = typename CBLQDepthFirstTraversalAccess<ndim>::cblq_semiword_bitblock_t;
	using cblq_semiword_bitblock_citer_t = typename CBLQDepthFirstTraversalAccess<ndim>::cblq_semiword_bitblock_citer_t;

	using block_offset_t = typename CBLQTraversalBlocks<ndim>::block_offset_t;
	using block_size_t = typename CBLQTraversalBlocks<ndim>::block_size_t;
	static constexpr block_size_t BLOCK_SIZE = (1ULL << (ndim * level_from_bottom));
	static constexpr char BLOCK_CODE_TO_VISIT = (visit_present_blocks ? 0b01 : 0b00);
	static constexpr int SEMIWORDS_PER_BITBLOCK = CBLQDepthFirstTraversalAccess<ndim>::SEMIWORDS_PER_BITBLOCK;

	typedef decltype(((LevelProcessBlockFns*)nullptr)->template get_level_process_block_fn<level_from_bottom>()) LevelProcessBlockFn;
};

// General templated loop body
template<int ndim, int level_from_bottom, bool has_dense_suffix, bool visit_present_blocks, typename LevelProcessBlockFns>
class CBLQDepthFirstTraversalLoop :
		private CBLQDepthFirstTraversalConstants<ndim, level_from_bottom, visit_present_blocks, LevelProcessBlockFns>, // For constants (duh)
		private CBLQDepthFirstTraversalLoop<ndim, level_from_bottom-1, has_dense_suffix, visit_present_blocks, LevelProcessBlockFns> // For loop nesting recursion
{
private:
	using Constants = CBLQDepthFirstTraversalConstants<ndim, level_from_bottom, visit_present_blocks, LevelProcessBlockFns>;
	using cblq_word_t = typename Constants::cblq_word_t;
	using cblq_word_citer_t = typename Constants::cblq_word_citer_t;
	using LevelProcessBlockFn = typename Constants::LevelProcessBlockFn;
	using block_offset_t = typename Constants::block_offset_t;
	using block_size_t = typename Constants::block_size_t;
	using Constants::BLOCK_SIZE;
	using Constants::BLOCK_CODE_TO_VISIT;
public:
	CBLQDepthFirstTraversalLoop(const CBLQRegionEncoding<ndim> &cblq, LevelProcessBlockFns &level_process_block_fns) :
		CBLQDepthFirstTraversalLoop<ndim, level_from_bottom-1, has_dense_suffix, visit_present_blocks, LevelProcessBlockFns>(cblq, level_process_block_fns),
		process_block_fn(level_process_block_fns.template get_level_process_block_fn<level_from_bottom>())
	{
		CBLQDepthFirstTraversalAccess<ndim>::get_level_iter_bounds(cblq, level_from_bottom, level_it, level_end_it);
	}

	inline void process_next_word(block_offset_t block_offset = 0) {
		cblq_word_t next_word = *level_it;
		++level_it;

		for (int i = 0; i < CBLQRegionEncoding<ndim>::CODES_PER_WORD; i++) {
			const cblq_word_t code = (next_word >> (i * CBLQRegionEncoding<ndim>::BITS_PER_CODE)) & 0b11;
			if (code == 0b10) { // code == 2
				const block_offset_t next_block_offset = block_offset + i * BLOCK_SIZE;
				this->CBLQDepthFirstTraversalLoop<ndim, level_from_bottom-1, has_dense_suffix, visit_present_blocks, LevelProcessBlockFns>::process_next_word(next_block_offset);
			} else {
				if (code == BLOCK_CODE_TO_VISIT) { // code == 0 or 1
					const block_offset_t next_block_offset = block_offset + i * BLOCK_SIZE;
					process_block_fn(BLOCK_SIZE, next_block_offset);
				}
			}
		}
	}

private:
	LevelProcessBlockFn process_block_fn;
	cblq_word_citer_t level_it, level_end_it;
};

// Specialized loop body for the non-dense bottom layer
template<int ndim, bool visit_present_blocks, typename LevelProcessBlockFns>
class CBLQDepthFirstTraversalLoop<ndim, 0, false, visit_present_blocks, LevelProcessBlockFns> :
		private CBLQDepthFirstTraversalConstants<ndim, 0, visit_present_blocks, LevelProcessBlockFns> // For constants (duh)
{
private:
	static constexpr int level_from_bottom = 0;
	using Constants = CBLQDepthFirstTraversalConstants<ndim, level_from_bottom, visit_present_blocks, LevelProcessBlockFns>;
	using cblq_word_t = typename Constants::cblq_word_t;
	using cblq_word_citer_t = typename Constants::cblq_word_citer_t;
	using LevelProcessBlockFn = typename Constants::LevelProcessBlockFn;
	using block_offset_t = typename Constants::block_offset_t;
	using block_size_t = typename Constants::block_size_t;
	using Constants::BLOCK_SIZE;
	using Constants::BLOCK_CODE_TO_VISIT;
public:
	CBLQDepthFirstTraversalLoop(const CBLQRegionEncoding<ndim> &cblq, LevelProcessBlockFns &level_process_block_fns) :
		process_block_fn(level_process_block_fns.template get_level_process_block_fn<level_from_bottom>())
	{
		CBLQDepthFirstTraversalAccess<ndim>::get_level_iter_bounds(cblq, level_from_bottom, level_it, level_end_it);
	}

	inline void process_next_word(block_offset_t block_offset = 0) {
		cblq_word_t next_word = *level_it;
		++level_it;

		for (int i = 0; i < CBLQRegionEncoding<ndim>::CODES_PER_WORD; i++) {
			const cblq_word_t code = (next_word >> (i * CBLQRegionEncoding<ndim>::BITS_PER_CODE)) & 0b11;
			if (code == BLOCK_CODE_TO_VISIT) { // code == 0 or 1
				const block_offset_t next_block_offset = block_offset + i * BLOCK_SIZE;
				process_block_fn(BLOCK_SIZE, next_block_offset);
			}
		}
	}

private:
	LevelProcessBlockFn process_block_fn;
	cblq_word_citer_t level_it, level_end_it;
};

// Specialized loop body for the dense bottom layer
template<int ndim, bool visit_present_blocks, typename LevelProcessBlockFns>
class CBLQDepthFirstTraversalLoop<ndim, 0, true, visit_present_blocks, LevelProcessBlockFns> :
		private CBLQDepthFirstTraversalConstants<ndim, 0, visit_present_blocks, LevelProcessBlockFns> // For constants (duh)
{
private:
	static constexpr int level_from_bottom = 0;
	using Constants = CBLQDepthFirstTraversalConstants<ndim, level_from_bottom, visit_present_blocks, LevelProcessBlockFns>;
	using cblq_word_t = typename Constants::cblq_word_t;
	using cblq_semiword_t = typename Constants::cblq_semiword_t;
	using cblq_word_citer_t = typename Constants::cblq_word_citer_t;
	using cblq_semiword_bitblock_t = typename Constants::cblq_semiword_bitblock_t;
	using cblq_semiword_bitblock_citer_t = typename Constants::cblq_semiword_bitblock_citer_t;
	using LevelProcessBlockFn = typename Constants::LevelProcessBlockFn;
	using block_offset_t = typename Constants::block_offset_t;
	using block_size_t = typename Constants::block_size_t;
	using Constants::BLOCK_SIZE;
	using Constants::BLOCK_CODE_TO_VISIT;
	using Constants::SEMIWORDS_PER_BITBLOCK;
public:
	CBLQDepthFirstTraversalLoop(const CBLQRegionEncoding<ndim> &cblq, LevelProcessBlockFns &level_process_block_fns) :
		process_block_fn(level_process_block_fns.template get_level_process_block_fn<level_from_bottom>()),
		cur_bitblock_semiwords_left(0)
	{
		CBLQDepthFirstTraversalAccess<ndim>::get_level_dense_iter_bounds(cblq, num_semiwords, bitblock_it);
	}

	inline void process_next_word(block_offset_t block_offset = 0) {
		if (cur_bitblock_semiwords_left == 0)
			load_next_bitblock();

		for (int i = 0; i < CBLQRegionEncoding<ndim>::CODES_PER_WORD; i++) {
			const char code = (cur_bitblock >> i) & 0b1;
			if (code == BLOCK_CODE_TO_VISIT) {
				const block_offset_t next_block_offset = block_offset + i * BLOCK_SIZE;
				process_block_fn(BLOCK_SIZE, next_block_offset);
			}
		}

		--cur_bitblock_semiwords_left;
		cur_bitblock >>= CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD;
	}

private:
	inline void load_next_bitblock() {
		cur_bitblock = *bitblock_it;
		++bitblock_it;
		cur_bitblock_semiwords_left = SEMIWORDS_PER_BITBLOCK;
	}
private:
	LevelProcessBlockFn process_block_fn;

	uint64_t num_semiwords;
	cblq_semiword_bitblock_citer_t bitblock_it;
	cblq_semiword_bitblock_t cur_bitblock;
	int cur_bitblock_semiwords_left;

};

template<int ndim>
class CBLQDepthFirstTraversal {
private:
	// The highest level must have sufficient bits such that the end offset of the node at that level
	// is at most 2^(type bits - 1). Since we're assuming 64-bit offsets, we have 2^63 to work with.
	// At the lowest (leaf) level, block size is 1, so node size is 2^ndim, and each level above that is
	// 2^ndim larger. Thus, at level L, node size is 2^(ndim * (L+1)). Solving for the highest level supporting
	// node size 2^63 or less, we get (ndim * (L+1)) = 63 <-> L = 63/ndim - 1
	static constexpr int MAX_LEVELS = (std::numeric_limits<uint64_t>::digits - 1) / ndim - 1;

public:
	template<bool visit_present_blocks, typename LevelProcessBlockFns>
	static void traverse_df(const CBLQRegionEncoding<ndim> &cblq, LevelProcessBlockFns &level_process_block_fns) {

		if (CBLQDepthFirstTraversalAccess<ndim>::has_dense_suffix(cblq))
			traverse_df<true, MAX_LEVELS, visit_present_blocks, LevelProcessBlockFns>(cblq, level_process_block_fns);
		else
			traverse_df<false, MAX_LEVELS, visit_present_blocks, LevelProcessBlockFns>(cblq, level_process_block_fns);
	}

private:
	template<bool has_dense_suffix, int level_from_bottom, bool visit_present_blocks, typename LevelProcessBlockFns>
	static void traverse_df(const CBLQRegionEncoding<ndim> &cblq, LevelProcessBlockFns &level_process_block_fns) {
		using Constants = CBLQDepthFirstTraversalConstants<ndim, level_from_bottom, visit_present_blocks, LevelProcessBlockFns>;
		using block_offset_t = typename Constants::block_offset_t;

		CBLQDepthFirstTraversalLoop<ndim, level_from_bottom, has_dense_suffix, visit_present_blocks, LevelProcessBlockFns> loop(cblq, level_process_block_fns);
		loop.process_next_word();
	}
};

template<typename ProcessBlockFn>
class SimpleLevelProcessBlocksFns {
public:
	SimpleLevelProcessBlocksFns(ProcessBlockFn &fn) : fn(fn) {}

	template<int level_from_bottom>
	inline ProcessBlockFn & get_level_process_block_fn() { return fn; }
private:
	ProcessBlockFn &fn;
};

template<typename ProcessBlockFn, typename ProcessLeafBlockFn, int level_from_bottom>
class LeafOptimizeLevelProcessBlocksFnsHelper {
public:
	typedef ProcessBlockFn RetType;
	inline static ProcessBlockFn & get_level_process_block_fn(ProcessBlockFn &fn, ProcessLeafBlockFn & /*unused*/) { return fn; }
};

template<typename ProcessBlockFn, typename ProcessLeafBlockFn>
class LeafOptimizeLevelProcessBlocksFnsHelper<ProcessBlockFn, ProcessLeafBlockFn, 0> {
public:
	typedef ProcessLeafBlockFn RetType;
	inline static ProcessLeafBlockFn & get_level_process_block_fn(ProcessBlockFn & /*unused*/, ProcessLeafBlockFn &leaf_fn) { return leaf_fn; }
};

template<typename ProcessBlockFn, typename ProcessLeafBlockFn>
class LeafOptimizeLevelProcessBlocksFns {
public:
	LeafOptimizeLevelProcessBlocksFns(ProcessBlockFn &fn, ProcessLeafBlockFn &leaf_fn) : fn(fn), leaf_fn(leaf_fn) {}

	template<int level_from_bottom>
	inline typename LeafOptimizeLevelProcessBlocksFnsHelper<ProcessBlockFn, ProcessLeafBlockFn, level_from_bottom>::RetType & get_level_process_block_fn() {
		return LeafOptimizeLevelProcessBlocksFnsHelper<ProcessBlockFn, ProcessLeafBlockFn, level_from_bottom>::get_level_process_block_fn(fn, leaf_fn);
	}

	ProcessBlockFn &fn;
	ProcessLeafBlockFn &leaf_fn;
};

template<int ndim>
template<bool visit_present_blocks, typename ProcessBlockFn>
void CBLQTraversalBlocks<ndim>::traverse_df(const CBLQRegionEncoding<ndim> &cblq, ProcessBlockFn process_block_fn) {
	auto level_process_block_fns = SimpleLevelProcessBlocksFns<ProcessBlockFn>(process_block_fn); // Same process block fn for all levels
	CBLQDepthFirstTraversal<ndim>::template traverse_df<visit_present_blocks>(cblq, level_process_block_fns);
}

template<int ndim>
template<bool visit_present_blocks, typename ProcessBlockFn, typename ProcessLeafBlockFn>
void CBLQTraversalBlocks<ndim>::traverse_df_leafopt(const CBLQRegionEncoding<ndim> &cblq, ProcessBlockFn process_block_fn, ProcessLeafBlockFn process_leaf_block_fn) {
	auto level_process_block_fns = LeafOptimizeLevelProcessBlocksFns<ProcessBlockFn, ProcessLeafBlockFn>(process_block_fn, process_leaf_block_fn); // Same process block fn for all levels
	CBLQDepthFirstTraversal<ndim>::template traverse_df<visit_present_blocks>(cblq, level_process_block_fns);
}

template<int ndim>
template<bool visit_present_blocks, typename LevelProcessBlockFns>
void CBLQTraversalBlocks<ndim>::traverse_df_levelopt(const CBLQRegionEncoding<ndim> &cblq, LevelProcessBlockFns level_process_block_fns) {
	CBLQDepthFirstTraversal<ndim>::template traverse_df<visit_present_blocks>(cblq, level_process_block_fns);
}

#endif /* CBLQ_TRANVERSAL_DF_IMPL_HPP_ */
