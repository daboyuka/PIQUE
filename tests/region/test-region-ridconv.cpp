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
 * test-region-ridconv.cpp
 *
 *  Created on: Jun 3, 2014
 *      Author: David A. Boyuka II
 */

#include <cstdlib>
#include <cstdint>
#include <cassert>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/timer/timer.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/ii/ii.hpp"
#include "pique/region/ii/ii-encode.hpp"
#include "pique/region/cii/cii.hpp"
#include "pique/region/cii/cii-encode.hpp"
#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-encode.hpp"
#include "pique/region/wah/wah.hpp"
#include "pique/region/wah/wah-encode.hpp"

using boost::timer::cpu_timer;
using boost::timer::cpu_times;
static double walltime(const cpu_timer &timer) { return (double)timer.elapsed().wall * 1e-9; }

static const uint64_t region_size = 16;
static const uint64_t region_counts[] = {3, 4, 1, 3, 4, 1, 0, 0};
static const std::vector<uint32_t> expected_rids = {0, 1, 2, 7, 11, 12, 13, 14};

static const uint64_t RANDOM_REGION_SIZE = 1ULL<<12;
static const double RANDOM_REGION_PROB = 0.1;

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
static boost::shared_ptr< typename RegionEncoderT::RegionEncodingOutT >
make_random_region(typename RegionEncoderT::RegionEncoderConfig conf, uint64_t region_size, double prob, std::vector<uint32_t> &expected) {
    RegionEncoderT builder(conf, region_size);

    // Repeat until 2 consecutive 0's are encountered (the sentinel value)
    for (uint64_t i = 0; i < region_size; i++) {
    	bool present = ((double)std::rand() / RAND_MAX) < prob;
    	builder.push_bits(1, present);

    	if (present)
    		expected.push_back(i);
    }
    builder.finalize();

   return builder.to_region_encoding();
}

template<typename RegionEncodingT>
static void test_ridconv(std::string testname, boost::shared_ptr< RegionEncodingT > region, std::vector< uint32_t > expected_rids) {

	std::vector<uint32_t> rids;

	cpu_timer elemcount_timer;
	uint64_t elemcount = region->get_element_count();
	elemcount_timer.stop();
	assert(elemcount == expected_rids.size());

	cpu_timer unsorted_timer;
	region->convert_to_rids(rids, false, true);
	unsorted_timer.stop();
	std::sort(rids.begin(), rids.end());
	assert(rids == expected_rids);

	cpu_timer sorted_timer;
	region->convert_to_rids(rids, true, true);
	sorted_timer.stop();
	assert(rids == expected_rids);

	region->convert_to_rids(rids, true, false);
	assert(rids == expected_rids);

#ifdef VERBOSE_TESTS
	printf("Test %s passed! Sorted/unsorted time: %lf/%lf, elemcount time: %lf\n", testname.c_str(), walltime(sorted_timer), walltime(unsorted_timer), walltime(elemcount_timer));
#endif
}

template<typename RegionEncoderT>
static void test_ridconv(std::string testname, typename RegionEncoderT::RegionEncoderConfig conf) {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;

	boost::shared_ptr< RegionEncodingT > region = make_region<RegionEncoderT>(conf, region_size, region_counts);

	std::vector<uint32_t> random_expected_rids;
	boost::shared_ptr< RegionEncodingT > random_region = make_random_region<RegionEncoderT>(conf, RANDOM_REGION_SIZE, RANDOM_REGION_PROB, random_expected_rids);

	std::vector<uint32_t> inv_random_expected_rids;
	boost::shared_ptr< RegionEncodingT > inv_random_region = make_random_region<RegionEncoderT>(conf, RANDOM_REGION_SIZE, 1 - RANDOM_REGION_PROB, inv_random_expected_rids);

	test_ridconv(testname + "-small", region, expected_rids);
	test_ridconv(testname + "-big-sparse", random_region, random_expected_rids);
	test_ridconv(testname + "-big-dense", inv_random_region, inv_random_expected_rids);

#ifdef VERBOSE_TESTS
	printf("Test %s passed!\n", testname.c_str());
#endif
}

int main(int argc, char **argv) {
	test_ridconv< IIRegionEncoder >("ii", IIRegionEncoderConfig());
	test_ridconv< CIIRegionEncoder >("cii", CIIRegionEncoderConfig());
	test_ridconv< CBLQRegionEncoder<2> >("cblq2d", CBLQRegionEncoderConfig(false));
	test_ridconv< CBLQRegionEncoder<2> >("cblq2d-dense", CBLQRegionEncoderConfig(true));
	test_ridconv< CBLQRegionEncoder<3> >("cblq3d", CBLQRegionEncoderConfig(false));
	test_ridconv< CBLQRegionEncoder<3> >("cblq3d-dense", CBLQRegionEncoderConfig(true));
	test_ridconv< WAHRegionEncoder >("wah", WAHRegionEncoderConfig());
}




