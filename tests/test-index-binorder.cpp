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
 * test-index-binorder.cpp
 *
 *  Created on: Jun 4, 2014
 *      Author: David A. Boyuka II
 */

#include <cstdint>
#include <cassert>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/data/grid.hpp"
#include "pique/data/dataset.hpp"
#include "pique/region/region-encoding.hpp"
#include "pique/region/ii/ii.hpp"
#include "pique/region/ii/ii-encode.hpp"
#include "pique/indexing/binned-index.hpp"
#include "pique/indexing/index-builder.hpp"
#include "pique/util/universal-value.hpp"

static const std::vector<double>	DATASET_1C = {1.0, -0.0, 0.0, -1.0};
static const std::vector<int>		DATASET_2C = {1, -0, 0, -1};
static const std::vector<uint16_t>	DATASET_US = {1, 0, 0, 2};

template<typename RegionEncoderT, typename DatatypeT>
static void
test_index(typename RegionEncoderT::RegionEncoderConfig conf, const std::vector< DatatypeT > &data, const std::vector< uint64_t > &expected_bins) {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;
	using SigbitsBinningSpec = SigbitsBinningSpecification< DatatypeT >;
	using BinningKeyType = typename SigbitsBinningSpec::QKeyType;
	BOOST_STATIC_ASSERT(boost::is_same< BinningKeyType, uint64_t >::value);

	InMemoryDataset< DatatypeT > dataset((std::vector< DatatypeT >(data)), Grid{data.size()});

	IndexBuilder< DatatypeT, RegionEncoderT, SigbitsBinningSpec > builder(conf, boost::make_shared< SigbitsBinningSpec >(sizeof(DatatypeT)*8));
	boost::shared_ptr< BinnedIndex > index_ptr = builder.build_index(dataset);

	assert(index_ptr->get_num_bins() == expected_bins.size());
	for (BinnedIndexTypes::bin_id_t i = 0; i < index_ptr->get_num_bins(); i++)
		assert(boost::get< BinningKeyType >(index_ptr->get_bin_key(i)) == expected_bins[i]);
}

int main(int argc, char **argv) {
	test_index< IIRegionEncoder, uint16_t >(IIRegionEncoderConfig(), DATASET_US, { 0UL, 1UL, 2UL });
	test_index< IIRegionEncoder, int      >(IIRegionEncoderConfig(), DATASET_2C, { 0xFFFFFFFFUL, 0UL, 1UL });
	test_index< IIRegionEncoder, double   >(IIRegionEncoderConfig(), DATASET_1C, { (1ULL << 63) | (1023UL << 52), (1ULL << 63), 0, (1023UL << 52) });
}




