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
 * test-cii-decode.cpp
 *
 *  Created on: Aug 21, 2015
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

#include "pique/region/cii/cii.hpp"
#include "pique/region/cii/cii-encode.hpp"
#include "pique/region/cii/cii-decoder.hpp"
#include "pique/setops/cii/cii-setops.hpp"

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

template<typename RegionEncoderT>
static boost::shared_ptr< typename RegionEncoderT::RegionEncodingOutT > make_region(typename RegionEncoderT::RegionEncoderConfig conf, const boost::dynamic_bitset<> &bitmap) {
	RegionEncoderT encoder(conf, bitmap.size());

	for (uint64_t i = 0; i < bitmap.size(); ++i)
		encoder.push_bits(1, bitmap[i]);

	encoder.finalize();
	return encoder.to_region_encoding();
}

static inline void do_test(boost::shared_ptr< CIIRegionEncoding > cii, const boost::dynamic_bitset<> &bitmap) {
	const uint64_t expected_rid_count = bitmap.count();
	uint64_t rid_count;

	// Both BaseProgressiveCIIDecoder and ProgressiveCIIDecoder should return the expected RID count (since it's a non-inverted CII)
	rid_count = 0;
	BaseProgressiveCIIDecoder bdecoder(*cii);
	while (bdecoder.has_top()) {
		assert(bitmap[bdecoder.top()]);
		bdecoder.next();
		++rid_count;
	}
	assert(rid_count == expected_rid_count);

	rid_count = 0;
	ProgressiveCIIDecoder decoder(*cii);
	while (decoder.has_top()) {
		assert(bitmap[decoder.top()]);
		decoder.next();
		++rid_count;
	}
	assert(rid_count == expected_rid_count);

	boost::shared_ptr< CIISetOperations > setops = boost::make_shared< CIISetOperations >(CIISetOperationsConfig());
	boost::shared_ptr< CIIRegionEncoding > inv_cii = setops->unary_set_op(cii, UnarySetOperation::COMPLEMENT);

	// Now BaseProgressiveCIIDecoder should still return the expected RID count, but ProgressiveCIIDecoder should return the inverted count
	rid_count = 0;
	BaseProgressiveCIIDecoder bdecoder2(*inv_cii);
	while (bdecoder2.has_top()) {
		assert(bitmap[bdecoder2.top()]);
		bdecoder2.next();
		++rid_count;
	}
	assert(rid_count == expected_rid_count);

	rid_count = 0;
	ProgressiveCIIDecoder decoder2(*inv_cii);
	while (decoder2.has_top()) {
		assert(!bitmap[decoder2.top()]);
		decoder2.next();
		++rid_count;
	}
	assert(rid_count == (cii->get_domain_size() - expected_rid_count));
}

int main(int argc, char **argv) {
	std::vector< std::reference_wrapper< boost::dynamic_bitset<> > > all_tests = { bitmap1, bitmap2, bitmap3, bigbitmap1, bigbitmap2, bigbitmap3 };

	for (const boost::dynamic_bitset<> &bitmap : all_tests) {
		boost::shared_ptr< CIIRegionEncoding > region = make_region< CIIRegionEncoder >(CIIRegionEncoderConfig(), bitmap);

		// Test compressed
		do_test(region, bitmap);

		// Test uncompressed
		region->decompress();
		do_test(region, bitmap);
	}
}




