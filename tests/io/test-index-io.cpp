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
#include <map>
#include <set>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/iterator/counting_iterator.hpp>

#include "pique/data/grid.hpp"
#include "pique/data/dataset.hpp"
#include "pique/region/region-encoding.hpp"
#include "pique/indexing/binned-index.hpp"
#include "pique/indexing/index-builder.hpp"
#include "pique/io/index-io.hpp"
#include "pique/io/posix/posix-index-io.hpp"
#include "pique/util/universal-value.hpp"

#include "pique/region/ii/ii.hpp"
#include "pique/region/ii/ii-encode.hpp"
#include "pique/region/cii/cii.hpp"
#include "pique/region/cii/cii-encode.hpp"
#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-encode.hpp"
#include "pique/region/wah/wah.hpp"
#include "pique/region/wah/wah-encode.hpp"

#include "make-index.hpp"
#include "write-and-verify-index.hpp"
#include "standard-datasets.hpp"

template<typename RegionEncoderT>
static void test_index_io(typename RegionEncoderT::RegionEncoderConfig conf, const std::vector<int> &domain, std::string indexfile = "test-index-io.idx") {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;
	using QKeyType = typename SigbitsBinningSpecification< int >::QKeyType;
	using region_id_t = BinnedIndexTypes::region_id_t;

	// Build an index to serialize
	boost::shared_ptr< BinnedIndex > index = make_index< RegionEncoderT, int >(conf, domain);

	write_and_verify_index(index, indexfile);

#ifdef VERBOSE_TESTS
	printf("Index IO test %s passed!\n", typeid(RegionEncodingT).name());
#endif
}

int main(int argc, char **argv) {
	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");

	std::string tempfile = tempdir + "/test-index-io.idx";

	test_index_io< IIRegionEncoder >(IIRegionEncoderConfig(), SMALL_DOMAIN, tempfile);
	test_index_io< CIIRegionEncoder >(CIIRegionEncoderConfig(), SMALL_DOMAIN, tempfile);
	test_index_io< WAHRegionEncoder >(WAHRegionEncoderConfig(), SMALL_DOMAIN, tempfile);
	test_index_io< CBLQRegionEncoder<2> >(CBLQRegionEncoderConfig(false), SMALL_DOMAIN, tempfile);
	test_index_io< CBLQRegionEncoder<2> >(CBLQRegionEncoderConfig(true), SMALL_DOMAIN, tempfile);

	srand(12345);
	std::vector<int> big_domain = make_big_domain(1ULL << 14, 30);

	test_index_io< IIRegionEncoder >(IIRegionEncoderConfig(), big_domain, tempfile);
	test_index_io< CIIRegionEncoder >(CIIRegionEncoderConfig(), big_domain, tempfile);
	test_index_io< WAHRegionEncoder >(WAHRegionEncoderConfig(), big_domain, tempfile);
	test_index_io< CBLQRegionEncoder<2> >(CBLQRegionEncoderConfig(false), big_domain, tempfile);
	test_index_io< CBLQRegionEncoder<2> >(CBLQRegionEncoderConfig(true), big_domain, tempfile);
}




