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
 * test-index-io.cpp
 *
 *  Created on: May 10, 2014
 *      Author: David A. Boyuka II
 */

#include <cstdint>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/dynamic_bitset.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/setops/setops.hpp"

#include "pique/region/ii/ii.hpp"
#include "pique/region/ii/ii-encode.hpp"
#include "pique/setops/ii/ii-setops.hpp"

#include "pique/region/cii/cii.hpp"
#include "pique/region/cii/cii-encode.hpp"
#include "pique/setops/cii/cii-setops.hpp"

using bitmaps_t = std::vector< std::reference_wrapper< boost::dynamic_bitset<> > >;
using ciis_t = std::vector< boost::shared_ptr< CIIRegionEncoding > >;

// Note: bitmap specified in big-endian order (least significant bit is the rightmost character in the string)
boost::dynamic_bitset<> bitmap1(std::string("1111000010111000"));
boost::dynamic_bitset<> bitmap2(std::string("1000100111110011"));
boost::dynamic_bitset<> bitmap3(std::string("1111111100000000"));

static boost::dynamic_bitset<> make_big_bitmap(uint64_t domain_size) {
	boost::dynamic_bitset<> bitmap(domain_size, 0);
	for (uint64_t i = 0; i < domain_size; i++)
		bitmap[i] = (rand() & 1);
	return bitmap;
}

boost::dynamic_bitset<> bigbitmap1 = make_big_bitmap(1ULL<<10);
boost::dynamic_bitset<> bigbitmap2 = make_big_bitmap(1ULL<<10);
boost::dynamic_bitset<> bigbitmap3 = make_big_bitmap(1ULL<<10);

boost::dynamic_bitset<> rlybigbitmap1 = make_big_bitmap(1ULL<<14);
boost::dynamic_bitset<> rlybigbitmap2 = make_big_bitmap(1ULL<<14);
boost::dynamic_bitset<> rlybigbitmap3 = make_big_bitmap(1ULL<<14);

//std::vector<int> bitmap1 = {0, 0, 0, 1, 1, 1, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1};
//	std::vector<int> bitmap2 = {1, 1, 0, 0, 1, 1, 1, 1, 1, 0, 0, 1, 0, 0, 0, 1};

template<typename RegionEncoderT>
static boost::shared_ptr< typename RegionEncoderT::RegionEncodingOutT > make_region(typename RegionEncoderT::RegionEncoderConfig conf, const boost::dynamic_bitset<> &bitmap) {
	RegionEncoderT encoder(conf, bitmap.size());

	for (uint64_t i = 0; i < bitmap.size(); ++i)
		encoder.push_bits(1, bitmap[i]);

	encoder.finalize();
	return encoder.to_region_encoding();
}

template<typename SetOps>
static inline void do_test_with_setops(ciis_t all_regions, const bitmaps_t &all_bitmaps) {
	SetOps setops(CIISetOperationsConfig(false));

	std::vector< uint32_t > rids;

	// A | B | C | ...
	boost::shared_ptr< CIIRegionEncoding > runion = setops.nary_set_op(all_regions.begin(), all_regions.end(), NArySetOperation::UNION);
	runion->convert_to_rids(rids, true, true);
	auto it = rids.begin(), end_it = rids.end();

	for (size_t i = 0; i < all_bitmaps[0].get().size(); ++i) {
		bool expected = false;
		for (boost::dynamic_bitset<> bitmap : all_bitmaps) {
			if (bitmap[i]) {
				expected = true;
				break;
			}
		}

		if (expected) {
			assert(it != end_it && *it == i);
			++it;
		} else {
			assert(it == end_it || *it != i);
		}
	}

	// A & B & C & ...
	boost::shared_ptr< CIIRegionEncoding > rinter = setops.nary_set_op(all_regions.begin(), all_regions.end(), NArySetOperation::INTERSECTION);
	rinter->convert_to_rids(rids, true, true);
	it = rids.begin(); end_it = rids.end();

	for (size_t i = 0; i < all_bitmaps[0].get().size(); ++i) {
		bool expected = true;
		for (boost::dynamic_bitset<> bitmap : all_bitmaps) {
			if (!bitmap[i]) {
				expected = false;
				break;
			}
		}

		if (expected) {
			assert(it != end_it && *it == i);
			++it;
		} else {
			assert(it == end_it || *it != i);
		}
	}

	// Invert all operands
	for (auto region : all_regions) setops.inplace_unary_set_op(region, UnarySetOperation::COMPLEMENT);

	// ~A | ~B | ~C | ...

	boost::shared_ptr< CIIRegionEncoding > rnegunion = setops.nary_set_op(all_regions.begin(), all_regions.end(), NArySetOperation::UNION);
	rnegunion->convert_to_rids(rids, true, true);
	it = rids.begin(); end_it = rids.end();

	for (size_t i = 0; i < all_bitmaps[0].get().size(); ++i) {
		bool expected = false;
		for (boost::dynamic_bitset<> bitmap : all_bitmaps) {
			if (!bitmap[i]) {
				expected = true;
				break;
			}
		}

		if (expected) {
			assert(it != end_it && *it == i);
			++it;
		} else {
			assert(it == end_it || *it != i);
		}
	}

	// Re-invert all operands
	for (auto region : all_regions) setops.inplace_unary_set_op(region, UnarySetOperation::COMPLEMENT);
}

static inline void do_test_vary_setops(ciis_t all_regions, const bitmaps_t &all_bitmaps) {
	do_test_with_setops< CIISetOperations >(all_regions, all_bitmaps);
	do_test_with_setops< CIISetOperationsNAry >(all_regions, all_bitmaps);
}

static inline void do_test(const bitmaps_t &all_bitmaps) {
	std::vector< boost::shared_ptr< CIIRegionEncoding > > all_regions;
	for (boost::dynamic_bitset<> &bitmap : all_bitmaps) {
		all_regions.push_back(make_region< CIIRegionEncoder >(CIIRegionEncoderConfig(), bitmap));
	}

	for (int i = -1; i < (int)all_regions.size(); ++i) {
		if (i >= 0)
			all_regions[i]->decompress();
		do_test_vary_setops(all_regions, all_bitmaps);
	}
}

int main(int argc, char **argv) {
	do_test(bitmaps_t { bitmap1, bitmap2, bitmap3 });
	do_test(bitmaps_t { bigbitmap1, bigbitmap2, bigbitmap3 });
	do_test(bitmaps_t { rlybigbitmap1, rlybigbitmap2, rlybigbitmap3 });
}




