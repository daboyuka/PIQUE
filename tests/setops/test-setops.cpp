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

#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-encode.hpp"
#include "pique/setops/cblq/cblq-setops.hpp"

#include "pique/region/bitmap/bitmap.hpp"
#include "pique/region/bitmap/bitmap-encode.hpp"
#include "pique/setops/bitmap/bitmap-setops.hpp"

// Note: bitmap specified in big-endian order (least significant bit is the rightmost character in the string)
boost::dynamic_bitset<> bitmap1(std::string("1111000010111000"));
boost::dynamic_bitset<> bitmap2(std::string("1000100111110011"));

static boost::dynamic_bitset<> make_big_bitmap(uint64_t domain_size) {
	boost::dynamic_bitset<> bitmap(domain_size, 0);
	for (uint64_t i = 0; i < domain_size; i++)
		bitmap[i] = (rand() & 1);
	return bitmap;
}

boost::dynamic_bitset<> bigbitmap1 = make_big_bitmap(1ULL<<10);
boost::dynamic_bitset<> bigbitmap2 = make_big_bitmap(1ULL<<10);
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

template<typename RegionEncoderT, typename SetOperationsT>
static void do_unary_test(const std::string testname, boost::dynamic_bitset<> &testbitmap, typename RegionEncoderT::RegionEncoderConfig conf, typename SetOperationsT::SetOperationsConfig setops_conf, UnarySetOperation op) {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;

	std::vector<uint32_t> rids, expected_original_rids, expected_compl_rids;
	for (uint32_t rid = 0; rid < testbitmap.size(); ++rid) {
		switch (op) {
		case UnarySetOperation::COMPLEMENT:
			if (testbitmap[rid])
				expected_original_rids.push_back(rid);
			else
				expected_compl_rids.push_back(rid);
			break;
		}
	}

	boost::shared_ptr< RegionEncodingT > region = make_region< RegionEncoderT >(conf, testbitmap);

	// Out-of-place unary setop
	SetOperationsT setops(setops_conf);
	boost::shared_ptr< RegionEncodingT > result = setops.unary_set_op(region, op);

	// Assert that the unary setop is as expected
	rids.clear();
	result->convert_to_rids(rids, true, true);
	assert(rids == expected_compl_rids);

	// Assert that the complement of the complement is equal to the original region
	if (op == UnarySetOperation::COMPLEMENT) {
		boost::shared_ptr< RegionEncodingT > double_complement = setops.unary_set_op(result, op);

		rids.clear();
		double_complement->convert_to_rids(rids, true, true);
		assert(rids == expected_original_rids);
	}

	// In-place unary setop
	setops.inplace_unary_set_op(region, op);

	// Assert that the in-place unary setop is as expected
	rids.clear();
	region->convert_to_rids(rids, true, false);
	assert(rids == expected_compl_rids);

#ifdef VERBOSE_TESTS
	fprintf(stderr, "Passted test: %s\n", testname.c_str());
#endif
}

template<typename RegionEncoderT, typename SetOperationsT>
static void do_binary_test(const std::string testname, boost::dynamic_bitset<> &testbitmap1, boost::dynamic_bitset<> &testbitmap2, typename RegionEncoderT::RegionEncoderConfig conf, typename SetOperationsT::SetOperationsConfig setops_conf, NArySetOperation op) {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;

	assert(testbitmap1.size() == testbitmap2.size());
	std::vector<uint32_t> rids, expected_rids;
	for (uint32_t rid = 0; rid < testbitmap1.size(); ++rid) {
		switch (op) {
		case NArySetOperation::UNION:
			if (testbitmap1[rid] || testbitmap2[rid])
				expected_rids.push_back(rid);
			break;
		case NArySetOperation::INTERSECTION:
			if (testbitmap1[rid] && testbitmap2[rid])
				expected_rids.push_back(rid);
			break;
		case NArySetOperation::DIFFERENCE:
			if (testbitmap1[rid] && !testbitmap2[rid])
				expected_rids.push_back(rid);
			break;
		case NArySetOperation::SYMMETRIC_DIFFERENCE:
			if (testbitmap1[rid] ^ testbitmap2[rid])
				expected_rids.push_back(rid);
			break;
		}
	}

	boost::shared_ptr< RegionEncodingT > region1 = make_region< RegionEncoderT >(conf, testbitmap1);
	boost::shared_ptr< RegionEncodingT > region2 = make_region< RegionEncoderT >(conf, testbitmap2);

	// Out-of-place binary setop
	SetOperationsT setops(setops_conf);
	boost::shared_ptr< RegionEncodingT > result = setops.binary_set_op(region1, region2, op);

	// Assert that the complement is as expected
	rids.clear();
	result->convert_to_rids(rids, true, true);
	assert(rids == expected_rids);

	// In-place binary setop
	setops.inplace_binary_set_op(region1, region2, op);

	rids.clear();
	region1->convert_to_rids(rids, true, false);
	assert(rids == expected_rids);

#ifdef VERBOSE_TESTS
	fprintf(stderr, "Passted test: %s\n", testname.c_str());
#endif
}

static inline void do_unary_test_for_all_methods(const std::string base_testname, boost::dynamic_bitset<> &testbitmap, UnarySetOperation op) {
	do_unary_test< CBLQRegionEncoder<2>, CBLQSetOperationsFast<2> >("cblq2d-fast-" + base_testname, testbitmap, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_unary_test< CBLQRegionEncoder<3>, CBLQSetOperationsFast<3> >("cblq3d-fast-" + base_testname, testbitmap, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_unary_test< CBLQRegionEncoder<4>, CBLQSetOperationsFast<4> >("cblq4d-fast-" + base_testname, testbitmap, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_unary_test< CBLQRegionEncoder<2>, CBLQSetOperationsNAry3Fast<2> >("cblq2d-nary3fast-" + base_testname, testbitmap, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_unary_test< CBLQRegionEncoder<3>, CBLQSetOperationsNAry3Fast<3> >("cblq3d-nary3fast-" + base_testname, testbitmap, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_unary_test< CBLQRegionEncoder<4>, CBLQSetOperationsNAry3Fast<4> >("cblq4d-nary3fast-" + base_testname, testbitmap, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_unary_test< BitmapRegionEncoder, BitmapSetOperations >("bitmap-" + base_testname, testbitmap, BitmapRegionEncoderConfig(), BitmapSetOperationsConfig(), op);
	do_unary_test< IIRegionEncoder, IISetOperations >("ii-" + base_testname, testbitmap, IIRegionEncoderConfig(), IISetOperationsConfig(), op);
	do_unary_test< CIIRegionEncoder, CIISetOperations >("cii-" + base_testname, testbitmap, CIIRegionEncoderConfig(), CIISetOperationsConfig(), op);
}

static inline void do_binary_test_for_all_methods(const std::string base_testname, boost::dynamic_bitset<> &testbitmap1, boost::dynamic_bitset<> &testbitmap2, NArySetOperation op) {
	do_binary_test< CBLQRegionEncoder<2>, CBLQSetOperationsFast<2> >("cblq2d-fast-" + base_testname, testbitmap1, testbitmap2, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_binary_test< CBLQRegionEncoder<3>, CBLQSetOperationsFast<3> >("cblq3d-fast-" + base_testname, testbitmap1, testbitmap2, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_binary_test< CBLQRegionEncoder<4>, CBLQSetOperationsFast<4> >("cblq4d-fast-" + base_testname, testbitmap1, testbitmap2, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_binary_test< CBLQRegionEncoder<2>, CBLQSetOperationsNAry3Fast<2> >("cblq2d-nary3fast-" + base_testname, testbitmap1, testbitmap2, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_binary_test< CBLQRegionEncoder<3>, CBLQSetOperationsNAry3Fast<3> >("cblq3d-nary3fast-" + base_testname, testbitmap1, testbitmap2, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_binary_test< CBLQRegionEncoder<4>, CBLQSetOperationsNAry3Fast<4> >("cblq4d-nary3fast-" + base_testname, testbitmap1, testbitmap2, CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true), op);
	do_binary_test< BitmapRegionEncoder, BitmapSetOperations >("bitmap-" + base_testname, testbitmap1, testbitmap2, BitmapRegionEncoderConfig(), BitmapSetOperationsConfig(), op);
	do_binary_test< IIRegionEncoder, IISetOperations >("ii-" + base_testname, testbitmap1, testbitmap2, IIRegionEncoderConfig(), IISetOperationsConfig(), op);
	do_binary_test< CIIRegionEncoder, CIISetOperations >("cii-" + base_testname, testbitmap1, testbitmap2, CIIRegionEncoderConfig(), CIISetOperationsConfig(), op);
}

int main(int argc, char **argv) {
	do_unary_test_for_all_methods("complement", bitmap1, UnarySetOperation::COMPLEMENT);

	do_binary_test_for_all_methods("union", bitmap1, bitmap2, NArySetOperation::UNION);
	do_binary_test_for_all_methods("intersection", bitmap1, bitmap2, NArySetOperation::INTERSECTION);
	do_binary_test_for_all_methods("bigunion", bigbitmap1, bigbitmap2, NArySetOperation::UNION);
}




