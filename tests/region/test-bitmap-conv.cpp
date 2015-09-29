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
#include <boost/iterator/function_input_iterator.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/timer/timer.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-encode.hpp"
#include "pique/region/bitmap/bitmap.hpp"

#include "pique/convert/cblq/cblq-to-bitmap-convert.hpp"

using boost::timer::cpu_timer;
using boost::timer::cpu_times;
static double walltime(const cpu_timer &timer) { return (double)timer.elapsed().wall * 1e-9; }

typedef boost::dynamic_bitset< BitmapRegionEncoding::block_t > Bitset;

static Bitset make_large_bitmap(uint64_t nbits, uint64_t seed = boost::mt19937_64::default_seed);
static Bitset make_large_sparse_bitmap(uint64_t nbits, int iters);

std::string small_bitmap_str = "0001110100001111"; // written in big-endian: bits are 1111 0000 1011 1000 in little-endian
const Bitset small_bitmap(small_bitmap_str);
const Bitset large_bitmap = make_large_bitmap(1ULL<<14);
const Bitset uneven_bitmap = make_large_bitmap(27 + (1ULL<<14));
const Bitset very_uneven_bitmap = make_large_bitmap(27 + (1ULL<<13));
const Bitset large_sparse_bitmap = make_large_sparse_bitmap(27 + (1ULL<<13), 7); // 4 -> density ~= 2^-7

static Bitset make_large_bitmap(uint64_t nbits, uint64_t seed) {
	boost::mt19937_64 rand; // default seed is fine

	Bitset b(
		boost::make_function_input_iterator(rand, (uint64_t)0),
		boost::make_function_input_iterator(rand, nbits / std::numeric_limits< Bitset::block_type >::digits));
	b.resize(nbits);
	return b;
}

static Bitset make_large_sparse_bitmap(uint64_t nbits, int iters) {
	uint64_t seed = boost::mt19937_64::default_seed;
	Bitset b = make_large_bitmap(nbits, seed);
	for (int i = 1; i < iters; ++i) {
		seed *= seed;
		Bitset b2 = make_large_bitmap(nbits, seed);
		b &= b2;
	}
	return b;
}

template<typename RegionEncoderT>
static boost::shared_ptr< typename RegionEncoderT::RegionEncodingOutT > make_region(typename RegionEncoderT::RegionEncoderConfig conf, const Bitset &bitmap) {
	RegionEncoderT encoder(conf, bitmap.size());

	for (size_t i = 0; i < bitmap.size(); i++) {
		encoder.push_bits(1, bitmap[i]);
	}

	encoder.finalize();
	return encoder.to_region_encoding();
}

template<typename RegionEncoderT, typename RegionConverterT>
static void do_bitmap_test(const std::string testname, typename RegionEncoderT::RegionEncoderConfig conf, const Bitset &bitmap) {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;

	RegionConverterT converter;
	boost::dynamic_bitset< BitmapRegionEncoding::block_t > actual_bitmap;

	// Generate the input region (non-bitmap RE)
	boost::shared_ptr< RegionEncodingT > region = make_region< RegionEncoderT >(conf, bitmap);

	// Convert to bitmap (timed)
	cpu_timer convert_timer;
	boost::shared_ptr< BitmapRegionEncoding > bitmap_region = converter.convert(region);
	convert_timer.stop();

	// Check correctness
	bitmap_region->to_bitset(actual_bitmap);
	assert(actual_bitmap == bitmap);

	// Clear
	bitmap_region->zero();

	// In-place union to bitmap (timed)
	cpu_timer ipunion_timer;
	converter.inplace_convert(region, bitmap_region, NArySetOperation::UNION);
	ipunion_timer.stop();

	// Check correctness
	bitmap_region->to_bitset(actual_bitmap);
	assert(actual_bitmap == bitmap);

	// Clear
	bitmap_region->fill();

	// In-place union to bitmap (timed)
	cpu_timer ipinter_timer;
	converter.inplace_convert(region, bitmap_region, NArySetOperation::INTERSECTION);
	ipinter_timer.stop();

	// Check correctness
	bitmap_region->to_bitset(actual_bitmap);
	assert(actual_bitmap == bitmap);

#ifdef VERBOSE_TESTS
	printf("Test \"%s\" passed! Time convert/ip-union/ip-inter: %lf/%lf/%lf\n", testname.c_str(), walltime(convert_timer), walltime(ipunion_timer), walltime(ipinter_timer));
#endif
}

int main(int argc, char **argv) {
	do_bitmap_test< CBLQRegionEncoder<2>, CBLQToBitmapConverter<2> >("small-cblq", CBLQRegionEncoderConfig(true), small_bitmap);
	do_bitmap_test< CBLQRegionEncoder<2>, CBLQToBitmapConverter<2> >("large-cblq", CBLQRegionEncoderConfig(true), large_bitmap);
	do_bitmap_test< CBLQRegionEncoder<2>, CBLQToBitmapConverter<2> >("uneven-cblq", CBLQRegionEncoderConfig(true), uneven_bitmap);
	do_bitmap_test< CBLQRegionEncoder<2>, CBLQToBitmapConverter<2> >("very-uneven-cblq", CBLQRegionEncoderConfig(true), very_uneven_bitmap);
	do_bitmap_test< CBLQRegionEncoder<2>, CBLQToBitmapConverter<2> >("large-sparse-cblq", CBLQRegionEncoderConfig(true), large_sparse_bitmap);

	do_bitmap_test< CBLQRegionEncoder<2>, CBLQToBitmapDFConverter<2> >("small-cblq-df", CBLQRegionEncoderConfig(true), small_bitmap);
	do_bitmap_test< CBLQRegionEncoder<2>, CBLQToBitmapDFConverter<2> >("large-cblq-df", CBLQRegionEncoderConfig(true), large_bitmap);
	do_bitmap_test< CBLQRegionEncoder<2>, CBLQToBitmapDFConverter<2> >("uneven-cblq-df", CBLQRegionEncoderConfig(true), uneven_bitmap);
	do_bitmap_test< CBLQRegionEncoder<2>, CBLQToBitmapDFConverter<2> >("very-uneven-cblq-df", CBLQRegionEncoderConfig(true), very_uneven_bitmap);
	do_bitmap_test< CBLQRegionEncoder<2>, CBLQToBitmapDFConverter<2> >("large-sparse-cblq-df", CBLQRegionEncoderConfig(true), large_sparse_bitmap);
}




