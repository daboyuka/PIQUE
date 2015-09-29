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
 * bitmap-engine.cpp
 *
 *  Created on: Aug 7, 2014
 *      Author: David A. Boyuka II
 */

#include <iostream>
#include <cassert>
#include <iterator>
#include <algorithm>
#include <set>

#include <boost/iterator/counting_iterator.hpp>

#include "pique/query/bitmap-query-engine.hpp"
#include "pique/stats/stats.hpp"

inline static bool is_bitmap(const boost::shared_ptr< RegionEncoding > &re) { return re->get_type() == RegionEncoding::Type::UNCOMPRESSED_BITMAP; }

boost::shared_ptr< RegionEncoding >
BitmapQueryEngine::evaluate_set_op(boost::shared_ptr< RegionEncoding > region, bool is_region_mutable, UnarySetOperation op) const {
	// Convert the input region into a bitmap, if it's not already
	if (is_bitmap(region)) {
		boost::shared_ptr< BitmapRegionEncoding > bitmap_region = boost::static_pointer_cast< BitmapRegionEncoding >(region);
		return is_region_mutable ?
			this->bitmap_setops->inplace_unary_set_op(bitmap_region, op) :
			this->bitmap_setops->unary_set_op(bitmap_region, op);
	} else {
		boost::shared_ptr< BitmapRegionEncoding > bitmap_region = this->bitmap_converter->convert(region);
		return this->bitmap_setops->inplace_unary_set_op(bitmap_region, op);
	}
}

boost::shared_ptr< RegionEncoding >
BitmapQueryEngine::evaluate_set_op(RegionEncodingPtrCIter begin, RegionEncodingPtrCIter end, MutabilityCIter mutability_begin, MutabilityCIter mutability_end, NArySetOperation op) const {
	if (std::distance(begin, end) == 1)
		return *begin;

	std::vector< boost::shared_ptr< RegionEncoding > > new_operands(begin, end);

	// Our goal is to get a mutable bitmap as the first region
	bool mutable_bitmap_first =
			*mutability_begin && is_bitmap(new_operands.front());

	// First, if needed, attempt swap a mutable bitmap to the first operand position if this set operation is symmetric
	if (!mutable_bitmap_first && is_set_op_symmetric(op)) {
		auto it = new_operands.begin();
		auto mut_it = mutability_begin;
		for (; it != new_operands.end(); ++it, ++mut_it) {
			// If found, put it first
			if (*mut_it && is_bitmap(*it)) {
				std::swap(new_operands.front(), *it);
				mutable_bitmap_first = true;
			}
		}
	}

	// Next, if still needed, convert the first region into a mutable bitmap
	if (!mutable_bitmap_first) {
		if (is_bitmap(new_operands.front())) {
			// Copy the bitmap
			new_operands.front() = boost::make_shared< BitmapRegionEncoding >(static_cast< BitmapRegionEncoding & >(*new_operands.front()));
		} else {
			// Convert to bitmap
			new_operands.front() = this->bitmap_converter->convert(new_operands.front());
		}
		mutable_bitmap_first = true;
	}

	assert(mutable_bitmap_first);

	// Finally, split all the operands into the first one, other bitmaps, and other non-bitmaps
	assert(is_set_op_tail_symmetric(op)); // Assume tail symmetry, so we can partition the tail operands as we like
	boost::shared_ptr< BitmapRegionEncoding > mutable_first_bitmap_operand;
	std::vector< boost::shared_ptr< const BitmapRegionEncoding > > other_bitmap_operands;
	std::vector< boost::shared_ptr< RegionEncoding > > other_nonbitmap_operands;

	auto it = new_operands.begin();
	mutable_first_bitmap_operand = boost::static_pointer_cast< BitmapRegionEncoding >(*it++);
	for (; it != new_operands.end(); ++it) {
		if (is_bitmap(*it))
			other_bitmap_operands.push_back(boost::static_pointer_cast< BitmapRegionEncoding >(*it));
		else
			other_nonbitmap_operands.push_back(*it);
	}

	// Now, use in-place bitmap conversion/setops to complete the set operation

	// N-ary setop, combine bitmaps into first bitmap
	this->bitmap_setops->inplace_nary_set_op(mutable_first_bitmap_operand, other_bitmap_operands.begin(), other_bitmap_operands.end(), op);

	// In-place bitmap conversion, combine non-bitmaps into first bitmap
	for (boost::shared_ptr< RegionEncoding > nonbitmap_operand : other_nonbitmap_operands)
		this->bitmap_converter->inplace_convert(nonbitmap_operand, mutable_first_bitmap_operand, op);

	// Return the final bitmap (phew)
	return mutable_first_bitmap_operand;
}

