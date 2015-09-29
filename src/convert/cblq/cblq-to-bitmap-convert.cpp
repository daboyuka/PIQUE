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
 * cblq-to-bitmap-convert.cpp
 *
 *  Created on: Jun 17, 2014
 *      Author: David A. Boyuka II
 */

#include <functional>

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/region/cblq/cblq-traversal.hpp"
#include "pique/convert/cblq/cblq-to-bitmap-convert.hpp"

// Assumes:
// bitcount < (bits in block_t)
// bits [offset, offset + bitcount) all fall in the same block_t
template<int bitcount, NArySetOperation combineop>
inline static void aligned_sub_byte_bitset(const uint64_t offset, const uint64_t /*voxel_size == bitcount*/, std::vector< BitmapRegionEncoding::block_t > &out_bits) {
	using block_t = BitmapRegionEncoding::block_t;

	const uint64_t blockpos = offset / BitmapRegionEncoding::BITS_PER_BLOCK;
	const int bitpos_in_block = offset % BitmapRegionEncoding::BITS_PER_BLOCK;
	const block_t mask = (((block_t)1 << bitcount) - 1) << bitpos_in_block;

	if (combineop == NArySetOperation::UNION)
		out_bits[blockpos] |= mask;
	else if (combineop == NArySetOperation::INTERSECTION)
		out_bits[blockpos] &= ~mask; // Note: the block we've been assigned is actually a 0-block in the CBLQ, so mask it out
	else if (combineop == NArySetOperation::DIFFERENCE)
		out_bits[blockpos] &= ~mask; // Note: we are on the right side of the difference
	else if (combineop == NArySetOperation::SYMMETRIC_DIFFERENCE)
		out_bits[blockpos] ^= mask;
}

// Assumes:
// sizeof(WordT) < sizeof(block_t)
// offset is a multiple of (bits in WordT)
template<typename WordT, NArySetOperation combineop>
inline static void aligned_word_bitset(const uint64_t offset, const uint64_t /*voxel_size == bits in WordT*/, std::vector< BitmapRegionEncoding::block_t > &out_bits) {
	using block_t = BitmapRegionEncoding::block_t;

	const uint64_t wordpos = offset / std::numeric_limits<WordT>::digits;
	WordT * const bits_as_words = reinterpret_cast<WordT * const>(&out_bits.front());

	if (combineop == NArySetOperation::UNION)
		bits_as_words[wordpos] = ~(WordT)0;
	else if (combineop == NArySetOperation::INTERSECTION)
		bits_as_words[wordpos] = (WordT)0; // Note: the block we've been assigned is actually a 0-block in the CBLQ, so mask it out
	else if (combineop == NArySetOperation::DIFFERENCE)
		bits_as_words[wordpos] = (WordT)0; // Note: we are on the right side of the difference
	else if (combineop == NArySetOperation::SYMMETRIC_DIFFERENCE)
		bits_as_words[wordpos] ^= ~(WordT)0;
}

// Assumes:
// offset is a multiple of (bits in block_t)
template<NArySetOperation combineop>
inline static void aligned_block_bitset(const uint64_t offset, const uint64_t count, std::vector< BitmapRegionEncoding::block_t > &out_bits) {
	using block_t = BitmapRegionEncoding::block_t;
	const uint64_t blockpos = offset / BitmapRegionEncoding::BITS_PER_BLOCK;
	const uint64_t nblocks = count / BitmapRegionEncoding::BITS_PER_BLOCK;

	auto start_it = out_bits.begin() + blockpos;
	auto end_it = start_it + nblocks;
	for (auto out_it = start_it; out_it != end_it; out_it++) {
		if (combineop == NArySetOperation::UNION)
			*out_it = ~(block_t)0;
		else if (combineop == NArySetOperation::INTERSECTION)
			*out_it = (block_t)0; // Note: the block we've been assigned is actually a 0-block in the CBLQ, so mask it out
		else if (combineop == NArySetOperation::DIFFERENCE)
			*out_it = (block_t)0; // Note: we are on the right side of the difference
		else if (combineop == NArySetOperation::SYMMETRIC_DIFFERENCE)
			*out_it ^= ~(block_t)0;
	}
}

// ASSUMPTION: Within a CBLQ, the region outside of the domain size will either be all black or all white; no mix
template<int ndim>
template<NArySetOperation combineop>
void CBLQToBitmapConverter<ndim>::inplace_convert(boost::shared_ptr<const CBLQRegionEncoding<ndim> > in_right, boost::shared_ptr< BitmapRegionEncoding > out_combine_left) const {
	using block_t = BitmapRegionEncoding::block_t;
	using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;
	using cblq_semiword_t = typename CBLQSemiwords<ndim>::cblq_semiword_t;

	using cblq_block_size_t = typename CBLQTraversalBlocks<ndim>::block_size_t;
	using cblq_block_offset_t = typename CBLQTraversalBlocks<ndim>::block_offset_t;

	constexpr bool visit_present_blocks = (combineop != NArySetOperation::INTERSECTION ? true : false);

	assert(in_right->get_domain_size() == out_combine_left->get_domain_size());

	const uint64_t block_aligned_domain_size = ((in_right->get_domain_size() - 1) / BitmapRegionEncoding::BITS_PER_BLOCK + 1) * BitmapRegionEncoding::BITS_PER_BLOCK;

	std::vector< block_t > &out_bits = out_combine_left->bits;

	auto exec_inner_loop_fn = [&out_bits, block_aligned_domain_size](int levels, int level, uint64_t level_len, cblq_block_size_t level_block_size, typename CBLQTraversalBlocks<ndim>::InnerBlockLoopParameters inner_block_loop_params) -> void {
		switch (level_block_size) {
		case 1:
		{
			auto process_block_fn = [&out_bits](cblq_block_size_t block_size, cblq_block_offset_t block_offset) {
				aligned_sub_byte_bitset<1, combineop>(block_offset, block_size, out_bits);
			};
			CBLQTraversalBlocks<ndim>::template traverse_bf_inner_loop<visit_present_blocks>(process_block_fn, inner_block_loop_params);
			break;
		}
		case 2:
		{
			auto process_block_fn = [&out_bits](cblq_block_size_t block_size, cblq_block_offset_t block_offset) {
				aligned_sub_byte_bitset<2, combineop>(block_offset, block_size, out_bits);
			};
			CBLQTraversalBlocks<ndim>::template traverse_bf_inner_loop<visit_present_blocks>(process_block_fn, inner_block_loop_params);
			break;
		}
		case 4:
		{
			auto process_block_fn = [&out_bits](cblq_block_size_t block_size, cblq_block_offset_t block_offset) {
				aligned_sub_byte_bitset<4, combineop>(block_offset, block_size, out_bits);
			};
			CBLQTraversalBlocks<ndim>::template traverse_bf_inner_loop<visit_present_blocks>(process_block_fn, inner_block_loop_params);
			break;
		}
		case 8:
		{
			auto process_block_fn = [&out_bits](cblq_block_size_t block_size, cblq_block_offset_t block_offset) {
				aligned_word_bitset<uint8_t, combineop>(block_offset, block_size, out_bits);
			};
			CBLQTraversalBlocks<ndim>::template traverse_bf_inner_loop<visit_present_blocks>(process_block_fn, inner_block_loop_params);
			break;
		}
		case 16:
		{
			auto process_block_fn = [&out_bits](cblq_block_size_t block_size, cblq_block_offset_t block_offset) {
				aligned_word_bitset<uint16_t, combineop>(block_offset, block_size, out_bits);
			};
			CBLQTraversalBlocks<ndim>::template traverse_bf_inner_loop<visit_present_blocks>(process_block_fn, inner_block_loop_params);
			break;
		}
		case 32:
		{
			auto process_block_fn = [&out_bits](cblq_block_size_t block_size, cblq_block_offset_t block_offset) {
				aligned_word_bitset<uint32_t, combineop>(block_offset, block_size, out_bits);
			};
			CBLQTraversalBlocks<ndim>::template traverse_bf_inner_loop<visit_present_blocks>(process_block_fn, inner_block_loop_params);
			break;
		}
		default:
		{
			if (level_block_size % std::numeric_limits<block_t>::digits != 0)
				abort();

			auto process_block_fn = [&out_bits, block_aligned_domain_size](cblq_block_size_t block_size, cblq_block_offset_t block_offset) {
				// Trim blocks to fit within the (block-aligned) domain
				// (this is only necessary in the multi-block case, since other cases stay within a block boundary,
				// because the region outside the domain is either all black or all white; see ASSUMPTION at top of function)
				if (block_size + block_offset > block_aligned_domain_size) {
					if (block_offset >= block_aligned_domain_size)
						return; // We're totally outside the domain, so do nothing
					else
						block_size = block_aligned_domain_size - block_offset; // We're partially inside the domain, so trim to that portion
				}
				aligned_block_bitset<combineop>(block_offset, block_size, out_bits);
			};
			CBLQTraversalBlocks<ndim>::template traverse_bf_inner_loop<visit_present_blocks>(process_block_fn, inner_block_loop_params);
			break;
		}
		}
	};

	CBLQTraversalBlocks<ndim>::template traverse_bf_outer_loop(*in_right, exec_inner_loop_fn);
}

template<int ndim>
void CBLQToBitmapConverter<ndim>::inplace_convert(boost::shared_ptr<const CBLQRegionEncoding<ndim> > in_right, boost::shared_ptr< BitmapRegionEncoding > out_combine_left, NArySetOperation combine_op) const {
	switch (combine_op) {
	case NArySetOperation::UNION:
		this->template inplace_convert<NArySetOperation::UNION>(in_right, out_combine_left);
		break;
	case NArySetOperation::INTERSECTION:
		this->template inplace_convert<NArySetOperation::INTERSECTION>(in_right, out_combine_left);
		break;
	case NArySetOperation::DIFFERENCE:
		this->template inplace_convert<NArySetOperation::DIFFERENCE>(in_right, out_combine_left);
		break;
	case NArySetOperation::SYMMETRIC_DIFFERENCE:
		this->template inplace_convert<NArySetOperation::SYMMETRIC_DIFFERENCE>(in_right, out_combine_left);
		break;
	}
}

template<int ndim>
inline boost::shared_ptr<BitmapRegionEncoding> CBLQToBitmapConverter<ndim>::convert(boost::shared_ptr< const CBLQRegionEncoding<ndim> > in) const {
	// Create a new, blank bitmap of sufficient length
	boost::shared_ptr< BitmapRegionEncoding > out = boost::make_shared< BitmapRegionEncoding >();
	BitmapRegionEncoding::allocate_nbits(*out, in->get_domain_size());

	// Call the in-place convert algorithm with the union combine op
	this->inplace_convert(in, out, NArySetOperation::UNION);

	// Return the now-populated bitmap
	return out;
}

// DF method

template<typename CBLQNodeBlockTypes::block_size_t block_size, NArySetOperation combineop>
struct SetBitsHelper {
	using bit_block_t = typename BitmapRegionEncoding::block_t;
	BOOST_STATIC_ASSERT(block_size % std::numeric_limits<bit_block_t>::digits == 0);
	inline static void do_bitset(typename CBLQNodeBlockTypes::block_size_t true_block_size, typename CBLQNodeBlockTypes::block_offset_t block_offset, std::vector< typename BitmapRegionEncoding::block_t > &out_bits) {
		aligned_block_bitset<combineop>(block_offset, true_block_size, out_bits);
	}
};
template<NArySetOperation combineop>
struct SetBitsHelper<1, combineop> {
	static constexpr CBLQNodeBlockTypes::block_size_t block_size = 1;
	inline static void do_bitset(typename CBLQNodeBlockTypes::block_size_t /*unused*/, typename CBLQNodeBlockTypes::block_offset_t block_offset, std::vector< typename BitmapRegionEncoding::block_t > &out_bits) {
		aligned_sub_byte_bitset<1, combineop>(block_offset, block_size, out_bits);
	}
};
template<NArySetOperation combineop>
struct SetBitsHelper<2, combineop> {
	static constexpr CBLQNodeBlockTypes::block_size_t block_size = 2;
	inline static void do_bitset(typename CBLQNodeBlockTypes::block_size_t /*unused*/, typename CBLQNodeBlockTypes::block_offset_t block_offset, std::vector< typename BitmapRegionEncoding::block_t > &out_bits) {
		aligned_sub_byte_bitset<2, combineop>(block_offset, block_size, out_bits);
	}
};
template<NArySetOperation combineop>
struct SetBitsHelper<4, combineop> {
	static constexpr CBLQNodeBlockTypes::block_size_t block_size = 4;
	inline static void do_bitset(typename CBLQNodeBlockTypes::block_size_t /*unused*/, typename CBLQNodeBlockTypes::block_offset_t block_offset, std::vector< typename BitmapRegionEncoding::block_t > &out_bits) {
		aligned_sub_byte_bitset<4, combineop>(block_offset, block_size, out_bits);
	}
};
template<NArySetOperation combineop>
struct SetBitsHelper<8, combineop> {
	static constexpr CBLQNodeBlockTypes::block_size_t block_size = 8;
	inline static void do_bitset(typename CBLQNodeBlockTypes::block_size_t /*unused*/, typename CBLQNodeBlockTypes::block_offset_t block_offset, std::vector< typename BitmapRegionEncoding::block_t > &out_bits) {
		aligned_word_bitset<uint8_t, combineop>(block_offset, block_size, out_bits);
	}
};
template<NArySetOperation combineop>
struct SetBitsHelper<16, combineop> {
	static constexpr CBLQNodeBlockTypes::block_size_t block_size = 16;
	inline static void do_bitset(typename CBLQNodeBlockTypes::block_size_t /*unused*/, typename CBLQNodeBlockTypes::block_offset_t block_offset, std::vector< typename BitmapRegionEncoding::block_t > &out_bits) {
		aligned_word_bitset<uint16_t, combineop>(block_offset, block_size, out_bits);
	}
};
template<NArySetOperation combineop>
struct SetBitsHelper<32, combineop> {
	static constexpr CBLQNodeBlockTypes::block_size_t block_size = 32;
	inline static void do_bitset(typename CBLQNodeBlockTypes::block_size_t /*unused*/, typename CBLQNodeBlockTypes::block_offset_t block_offset, std::vector< typename BitmapRegionEncoding::block_t > &out_bits) {
		aligned_word_bitset<uint32_t, combineop>(block_offset, block_size, out_bits);
	}
};
template<NArySetOperation combineop>
struct SetBitsHelper<64, combineop> {
	static constexpr CBLQNodeBlockTypes::block_size_t block_size = 64;
	inline static void do_bitset(typename CBLQNodeBlockTypes::block_size_t /*unused*/, typename CBLQNodeBlockTypes::block_offset_t block_offset, std::vector< typename BitmapRegionEncoding::block_t > &out_bits) {
		aligned_word_bitset<uint64_t, combineop>(block_offset, block_size, out_bits);
	}
};

template<typename CBLQNodeBlockTypes::block_size_t block_size, NArySetOperation combineop>
class SetBitsProcessBlockFn {
public:
	using bit_block_t = typename BitmapRegionEncoding::block_t;
	using cblq_block_size_t = typename CBLQNodeBlockTypes::block_size_t;
	using cblq_block_offset_t = typename CBLQNodeBlockTypes::block_offset_t;

	SetBitsProcessBlockFn(cblq_block_offset_t padded_domain_size, std::vector< typename BitmapRegionEncoding::block_t > &out_bits) :
		padded_domain_size(padded_domain_size), out_bits(out_bits)
	{}

	void operator()(cblq_block_size_t /*unused*/, cblq_block_offset_t block_offset) {
		uint64_t effective_block_size = block_size;
		if (block_size > std::numeric_limits<bit_block_t>::digits) { // Only for multi-block setting
			if (block_size + block_offset > padded_domain_size) {
				if (block_offset >= padded_domain_size)
					return; // We're totally outside the domain, so do nothing
				else
					effective_block_size = padded_domain_size - block_offset; // We're partially inside the domain, so trim to that portion
			}
		}
		SetBitsHelper<block_size, combineop>::do_bitset(effective_block_size, block_offset, out_bits);
	}
private:
	const uint64_t padded_domain_size;
	std::vector< typename BitmapRegionEncoding::block_t > &out_bits;
};

template<int ndim, NArySetOperation combineop>
class DFBitmapConvertBitsetFns {
public:
	DFBitmapConvertBitsetFns(uint64_t padded_domain_size, std::vector< typename BitmapRegionEncoding::block_t > &out_bits) :
		padded_domain_size(padded_domain_size), out_bits(out_bits)
	{}

	template<int level_from_bottom>
	SetBitsProcessBlockFn<1ULL << (ndim * level_from_bottom), combineop> get_level_process_block_fn() {
		return SetBitsProcessBlockFn<1ULL << (ndim * level_from_bottom), combineop>(padded_domain_size, out_bits);
	}
private:
	const uint64_t padded_domain_size;
	std::vector< typename BitmapRegionEncoding::block_t > &out_bits;
};

// ASSUMPTION: Within a CBLQ, the region outside of the domain size will either be all black or all white; no mix
template<int ndim>
template<NArySetOperation combineop>
void CBLQToBitmapDFConverter<ndim>::inplace_convert(boost::shared_ptr<const CBLQRegionEncoding<ndim> > in_right, boost::shared_ptr< BitmapRegionEncoding > out_combine_left) const {
	using block_t = BitmapRegionEncoding::block_t;
	using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;
	using cblq_semiword_t = typename CBLQSemiwords<ndim>::cblq_semiword_t;

	using cblq_block_size_t = typename CBLQNodeBlockTypes::block_size_t;
	using cblq_block_offset_t = typename CBLQNodeBlockTypes::block_offset_t;

	constexpr bool visit_present_blocks = (combineop != NArySetOperation::INTERSECTION ? true : false);

	assert(in_right->get_domain_size() == out_combine_left->get_domain_size());

	// TODO: FIX THIS: SUPPOSE BLOCKSIZE=64 and NDIM=3, THIS COULD CAUSE AN OVERRUN BY UP TO (2^3) = 8 BLOCKS PAST THE END
	// SOLUTION: PAD BY (2^3) BLOCKS, IT'S NOT THAT MUCH
	// Note: only the multi-block bitset case checks for overrun. The largest non-multi-block case is a single, whole block, and sets 2^ndim such blocks
	// Thus, we need to pad the bitmap 2^ndim blocks.
	const uint64_t domain_size_blocks = (in_right->get_domain_size() - 1) / BitmapRegionEncoding::BITS_PER_BLOCK + 1;
	const uint64_t blocks_to_pad = (1<<ndim);
	const uint64_t padded_blocks = domain_size_blocks + blocks_to_pad;
	const uint64_t padded_domain_size = padded_blocks * std::numeric_limits<block_t>::digits;
			//((in_right->get_domain_size() - 1) / BitmapRegionEncoding::BITS_PER_BLOCK + 1) * BitmapRegionEncoding::BITS_PER_BLOCK;

	std::vector< block_t > &out_bits = out_combine_left->bits;
	out_bits.resize(padded_blocks, 0);

	CBLQTraversalBlocks<ndim>::template traverse_df_levelopt<visit_present_blocks>(*in_right, DFBitmapConvertBitsetFns<ndim, combineop>(padded_domain_size, out_bits));
}

template<int ndim>
void CBLQToBitmapDFConverter<ndim>::inplace_convert(boost::shared_ptr<const CBLQRegionEncoding<ndim> > in_right, boost::shared_ptr< BitmapRegionEncoding > out_combine_left, NArySetOperation combine_op) const {
	switch (combine_op) {
	case NArySetOperation::UNION:
		this->template inplace_convert<NArySetOperation::UNION>(in_right, out_combine_left);
		break;
	case NArySetOperation::INTERSECTION:
		this->template inplace_convert<NArySetOperation::INTERSECTION>(in_right, out_combine_left);
		break;
	case NArySetOperation::DIFFERENCE:
		this->template inplace_convert<NArySetOperation::DIFFERENCE>(in_right, out_combine_left);
		break;
	case NArySetOperation::SYMMETRIC_DIFFERENCE:
		this->template inplace_convert<NArySetOperation::SYMMETRIC_DIFFERENCE>(in_right, out_combine_left);
		break;
	}
}

template<int ndim>
inline boost::shared_ptr<BitmapRegionEncoding> CBLQToBitmapDFConverter<ndim>::convert(boost::shared_ptr< const CBLQRegionEncoding<ndim> > in) const {
	using block_t = BitmapRegionEncoding::block_t;
	// Create a new, blank bitmap of sufficient length (padded as needed by the in-place method for efficiency)
	const uint64_t domain_size_blocks = (in->get_domain_size() - 1) / BitmapRegionEncoding::BITS_PER_BLOCK + 1;
	const uint64_t blocks_to_pad = (1<<ndim);
	const uint64_t padded_blocks = domain_size_blocks + blocks_to_pad;
	const uint64_t padded_domain_size = padded_blocks * std::numeric_limits<block_t>::digits;

	boost::shared_ptr< BitmapRegionEncoding > out = boost::make_shared< BitmapRegionEncoding >(padded_domain_size, false);
	out->domain_size = in->get_domain_size();

	// Call the in-place convert algorithm with the union combine op
	this->inplace_convert(in, out, NArySetOperation::UNION);

	// Return the now-populated bitmap
	return out;
}

// Explicit template instantiation for 2D-4D CBLQs
template class CBLQToBitmapConverter<2>;
template class CBLQToBitmapConverter<3>;
template class CBLQToBitmapConverter<4>;
template class CBLQToBitmapDFConverter<2>;
template class CBLQToBitmapDFConverter<3>;
template class CBLQToBitmapDFConverter<4>;
