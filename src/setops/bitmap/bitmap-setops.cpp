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
 * ii-setops.cpp
 *
 *  Created on: Mar 20, 2014
 *      Author: David A. Boyuka II
 */

#include <vector>

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/setops/bitmap/bitmap-setops.hpp"

boost::shared_ptr< BitmapRegionEncoding > BitmapSetOperations::unary_set_op_impl(boost::shared_ptr< const BitmapRegionEncoding > region, UnarySetOperation op) const {
	assert(op == UnarySetOperation::COMPLEMENT);

	boost::shared_ptr< BitmapRegionEncoding > out = boost::make_shared< BitmapRegionEncoding >();
	out->domain_size = region->domain_size;
	out->bits.resize(region->bits.size());

	auto in_it = region->bits.begin();
	auto out_it = out->bits.begin();
	for (; in_it != region->bits.end(); ++in_it, ++out_it)
		*out_it = ~*in_it;

	return out;
}

void BitmapSetOperations::inplace_unary_set_op_impl(boost::shared_ptr< BitmapRegionEncoding > region_and_out, UnarySetOperation op) const {
	assert(op == UnarySetOperation::COMPLEMENT);

	for (auto it = region_and_out->bits.begin(); it != region_and_out->bits.end(); ++it)
		*it = ~*it;
}

typedef BitmapRegionEncoding::block_t block_t;
typedef typename std::vector< BitmapRegionEncoding::block_t >::iterator block_it_t;
typedef typename std::vector< BitmapRegionEncoding::block_t >::const_iterator const_block_it_t;

template<NArySetOperation op>
inline static void apply_binary_op_inner(const block_t &left, const block_t &right, block_t &out);
template<> inline void apply_binary_op_inner<NArySetOperation::UNION>(const block_t &left, const block_t &right, block_t &out)					{ out = left | right; }
template<> inline void apply_binary_op_inner<NArySetOperation::INTERSECTION>(const block_t &left, const block_t &right, block_t &out)			{ out = left & right; }
template<> inline void apply_binary_op_inner<NArySetOperation::DIFFERENCE>(const block_t &left, const block_t &right, block_t &out)				{ out = left & ~right; }
template<> inline void apply_binary_op_inner<NArySetOperation::SYMMETRIC_DIFFERENCE>(const block_t &left, const block_t &right, block_t &out)	{ out = left ^ right; }

template<NArySetOperation op>
inline static void apply_binary_op(const_block_it_t left_it, const_block_it_t left_end_it, const_block_it_t right_it, block_it_t out_it) {
	for (; left_it != left_end_it; ++left_it, ++right_it, ++out_it)
		apply_binary_op_inner<op>(*left_it, *right_it, *out_it);
}

boost::shared_ptr< BitmapRegionEncoding > BitmapSetOperations::binary_set_op_impl(boost::shared_ptr< const BitmapRegionEncoding > left, boost::shared_ptr< const BitmapRegionEncoding > right, NArySetOperation op) const {
	assert(left->domain_size == right->domain_size);

	boost::shared_ptr< BitmapRegionEncoding > out = boost::make_shared< BitmapRegionEncoding >();
	out->domain_size = left->domain_size;
	out->bits.resize(left->bits.size());

	auto left_it = left->bits.cbegin();
	auto left_end_it = left->bits.cend();
	auto right_it = right->bits.cbegin();
	auto out_it = out->bits.begin();

	switch (op) {
	case NArySetOperation::UNION:
		apply_binary_op<NArySetOperation::UNION>(left_it, left_end_it, right_it, out_it);
		break;
	case NArySetOperation::INTERSECTION:
		apply_binary_op<NArySetOperation::INTERSECTION>(left_it, left_end_it, right_it, out_it);
		break;
	case NArySetOperation::DIFFERENCE:
		apply_binary_op<NArySetOperation::DIFFERENCE>(left_it, left_end_it, right_it, out_it);
		break;
	case NArySetOperation::SYMMETRIC_DIFFERENCE:
		apply_binary_op<NArySetOperation::SYMMETRIC_DIFFERENCE>(left_it, left_end_it, right_it, out_it);
		break;
	}

	return out;
}

template<NArySetOperation op>
inline static void apply_inplace_binary_op(block_it_t left_out_it, block_it_t left_out_end_it, const_block_it_t right_it) {
	for (; left_out_it != left_out_end_it; ++left_out_it, ++right_it)
		apply_binary_op_inner<op>(*left_out_it, *right_it, *left_out_it);
}

void BitmapSetOperations::inplace_binary_set_op_impl(boost::shared_ptr< BitmapRegionEncoding > left_and_out, boost::shared_ptr< const BitmapRegionEncoding > right, NArySetOperation op) const {
	assert(left_and_out->domain_size == right->domain_size);

	auto left_out_it = left_and_out->bits.begin();
	auto left_out_end_it = left_and_out->bits.end();
	auto right_it = right->bits.cbegin();

	switch (op) {
	case NArySetOperation::UNION:
		apply_inplace_binary_op<NArySetOperation::UNION>(left_out_it, left_out_end_it, right_it);
		break;
	case NArySetOperation::INTERSECTION:
		apply_inplace_binary_op<NArySetOperation::INTERSECTION>(left_out_it, left_out_end_it, right_it);
		break;
	case NArySetOperation::DIFFERENCE:
		apply_inplace_binary_op<NArySetOperation::DIFFERENCE>(left_out_it, left_out_end_it, right_it);
		break;
	case NArySetOperation::SYMMETRIC_DIFFERENCE:
		apply_inplace_binary_op<NArySetOperation::SYMMETRIC_DIFFERENCE>(left_out_it, left_out_end_it, right_it);
		break;
	}
}

void BitmapSetOperations::inplace_nary_set_op_impl(boost::shared_ptr< BitmapRegionEncoding > first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const {
	// Simple implementation: repeatedly apply the in-place binary setop
	// There's no efficiency loss for a linear approach vs. the heap-based approach, since bitmap binary
	// setop complexity is unaffected by region density, so this is the optimial way to implement N-ary in-place setops

	while (it != end_it)
		this->inplace_binary_set_op(first_and_out, *it++, op);
}

