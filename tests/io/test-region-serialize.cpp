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
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/ii/ii.hpp"
#include "pique/region/ii/ii-encode.hpp"
#include "pique/region/cii/cii.hpp"
#include "pique/region/cii/cii-encode.hpp"
#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-encode.hpp"
#include "pique/region/wah/wah.hpp"
#include "pique/region/wah/wah-encode.hpp"

const uint64_t region_size = 16;
const uint64_t region1_counts[] = {3, 4, 1, 3, 4, 1, 0, 0};
const uint64_t region2_counts[] = {4, 3, 1, 3, 4, 1, 0, 0};

template<typename RegionEncoderT>
static boost::shared_ptr< typename RegionEncoderT::RegionEncodingOutT >
make_region(typename RegionEncoderT::RegionEncoderConfig conf, uint64_t region_size, const uint64_t region_counts[]) {
    RegionEncoderT builder(conf, region_size);

    // Repeat until 2 consecutive 0's are encountered (the sentinel value)
    for (int i = 0; region_counts[i] || region_counts[i + 1]; i += 2) {
    	builder.push_bits(region_counts[i], true);
    	builder.push_bits(region_counts[i + 1], false);
    }
    builder.finalize();

   return builder.to_region_encoding();
}

template<typename RegionEncoderT>
static void test_serialization(typename RegionEncoderT::RegionEncoderConfig conf, uint64_t expected_serialized_size) {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;

	boost::shared_ptr< RegionEncodingT > correct = make_region<RegionEncoderT>(conf, region_size, region1_counts);
	boost::shared_ptr< RegionEncodingT > incorrect = make_region<RegionEncoderT>(conf, region_size, region2_counts);
	boost::shared_ptr< RegionEncodingT > deserailized = boost::make_shared<RegionEncodingT>();

	assert(*correct == *correct);
	assert(!(*correct == *incorrect));

    std::vector<char> buffer(1024, 0);

    boost::iostreams::stream < boost::iostreams::basic_array<char> > buffer_array(&buffer.front(), buffer.size());
	correct->save_to_stream(buffer_array);

	boost::iostreams::seek(buffer_array, 0, std::ios::cur);
	//uint64_t serialized_size = boost::iostreams::seek(buffer_array, 0, std::ios::cur);
	//assert(serialized_size == expected_serialized_size);

	boost::iostreams::seek(buffer_array, 0, std::ios::beg);
	deserailized->load_from_stream(buffer_array);

	assert(*correct == *deserailized);

#ifdef VERBOSE_TESTS
	printf("Test %s passed!\n", typeid(RegionEncodingT).name());
#endif
}

int main(int argc, char **argv) {
	test_serialization< IIRegionEncoder >(IIRegionEncoderConfig(), (uint64_t)(8 + 8 * 4));
	test_serialization< CIIRegionEncoder >(CIIRegionEncoderConfig(), (uint64_t)(37));
	test_serialization< CBLQRegionEncoder<2> >(CBLQRegionEncoderConfig(false), (uint64_t)(8 + 1 * 5));
	test_serialization< CBLQRegionEncoder<2> >(CBLQRegionEncoderConfig(true), (uint64_t)(8 + 1 * 1 + 1 * 4 / 2));
	test_serialization< WAHRegionEncoder >(WAHRegionEncoderConfig(), (uint64_t)(0));
}




