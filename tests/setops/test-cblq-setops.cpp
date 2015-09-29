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
#include <boost/timer/timer.hpp>

#include "pique/data/grid.hpp"
#include "pique/data/dataset.hpp"
#include "pique/region/region-encoding.hpp"
#include "pique/indexing/binned-index.hpp"
#include "pique/indexing/index-builder.hpp"
#include "pique/setops/setops.hpp"

#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-encode.hpp"
#include "pique/setops/cblq/cblq-setops.hpp"

#include "standard-datasets.hpp"

using boost::timer::cpu_timer;
using boost::timer::cpu_times;
static double walltime(const cpu_timer &timer) { return (double)timer.elapsed().wall * 1e-9; }

static boost::shared_ptr< InMemoryDataset<int> >
make_dataset(const std::vector<int> &domain) {
	boost::shared_ptr< InMemoryDataset<int> > dataset = boost::make_shared< InMemoryDataset<int> >((std::vector<int>(domain)), Grid{domain.size()});

	return dataset;
}

template<typename RegionEncoderT>
static boost::shared_ptr< BinnedIndex >
make_index(typename RegionEncoderT::RegionEncoderConfig conf, boost::shared_ptr< InMemoryDataset<int> > dataset) {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;
	using SigbitsBinningSpec = SigbitsBinningSpecification< int >;

	IndexBuilder< int, RegionEncoderT, SigbitsBinningSpec > builder(conf, boost::make_shared< SigbitsBinningSpec >(sizeof(int)*8));
	return builder.build_index(*dataset);
}

struct TestCase {
	TestCase(const std::string testname, const std::vector<int> &domain, const std::vector< std::pair<int, int> > &&bin_ranges, NArySetOperation query_combine_op) :
		testname(testname), domain(domain), bin_ranges(bin_ranges), query_combine_op(query_combine_op)
	{}

	const std::string testname;
	const std::vector<int> &domain;
	const std::vector< std::pair<int, int> > bin_ranges;
	const NArySetOperation query_combine_op;
};

template<typename RegionEncoderT, typename SetOperationsT>
static boost::shared_ptr< RegionEncoding > do_test(const TestCase &test, std::string setopsimpl, typename RegionEncoderT::RegionEncoderConfig conf, typename SetOperationsT::SetOperationsConfig setops_conf) {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;

	boost::shared_ptr< SetOperationsT > setops = boost::make_shared< SetOperationsT >(setops_conf);

	boost::shared_ptr< InMemoryDataset<int> > dataset = make_dataset(test.domain);
	boost::shared_ptr< BinnedIndex > index = make_index< RegionEncoderT >(conf, dataset);

	std::vector< boost::shared_ptr< RegionEncoding > > index_regions;
	index->get_regions(index_regions);

	cpu_timer binmerge_timer;
	std::vector< boost::shared_ptr< RegionEncoding > > terms;
	for (auto constr_it = test.bin_ranges.begin(); constr_it != test.bin_ranges.end(); ++constr_it) {
		const std::pair<int, int> bin_range = *constr_it;
		boost::shared_ptr< RegionEncoding > term =
				setops->dynamic_nary_set_op(
						index_regions.begin() + bin_range.first,
						index_regions.begin() + bin_range.second + 1,
						NArySetOperation::UNION);
		terms.push_back(term);
	}
	binmerge_timer.stop();

	cpu_timer termops_timer;
	boost::shared_ptr< RegionEncoding > result =
					setops->dynamic_nary_set_op(
							terms.begin(),
							terms.end(),
							test.query_combine_op);
	termops_timer.stop();

#ifdef VERBOSE_TESTS
	std::cout << "[STATS] Test \"" << test.testname << "\" on setops impl. " << setopsimpl << ": " <<
					"binmerge=" << binmerge_timer.realTime() << ", " <<
					"termops=" << termops_timer.realTime() << std::endl;
#endif

	return result;
}

std::vector<int> && make_big_domain(uint64_t domain_size, int value_range, std::vector<int> &&domain_out) {
	uint64_t i = 0;
	for (; i < (uint64_t)value_range; i++)
		domain_out.push_back(i); // Ensure each key appears at least once
	for (; i < domain_size; i++)
		domain_out.push_back(rand() % value_range); // Fill the rest with randomness

	return static_cast<std::vector<int> &&>(domain_out);
}

// Bin bounds are inclusive, since they represent a [lkey, hkey) range, and even an exclusive hkey implies an inclusive hkey bin read
const std::vector< TestCase > TEST_CASES = {
	TestCase("small-union", SMALL_DOMAIN, std::vector< std::pair<int, int> >{{0, 0}, {2, 2}}, NArySetOperation::UNION),
	TestCase("small-overlap-union", SMALL_DOMAIN, std::vector< std::pair<int, int> >{{0, 1}, {0, 0}}, NArySetOperation::UNION),
	TestCase("small-inter", SMALL_DOMAIN, std::vector< std::pair<int, int> >{{0, 1}, {1, 2}}, NArySetOperation::INTERSECTION),
	TestCase("med-union", MEDIUM_DOMAIN, std::vector< std::pair<int, int> >{{0, 0}, {2, 3}, {5, 5}}, NArySetOperation::UNION),
	TestCase("med-overlap-union", MEDIUM_DOMAIN, std::vector< std::pair<int, int> >{{0, 1}, {1, 5}}, NArySetOperation::UNION),
	TestCase("med-inter", MEDIUM_DOMAIN, std::vector< std::pair<int, int> >{{0, 3}, {3, 5}}, NArySetOperation::INTERSECTION),
	TestCase("big-inter", BIG_DOMAIN, std::vector< std::pair<int, int> >{{2, 6}, {5, 9}}, NArySetOperation::INTERSECTION),
	TestCase("manybin-inter", MANYBINS_DOMAIN, std::vector< std::pair<int, int> >{{60, 120}, {80, 140}}, NArySetOperation::INTERSECTION),
};

// test case small-union:
// [0, 0] = 2202 1110 0001 1101 (in big-endian)
// [2, 2] = 2020 0001 1110 (in big-endian)
// final result = 2222 1111 0001 1110 1101 (big-endian, before compact)

// test case small-inter:
// [0, 1] = 2121 1110 0001 (in big-endian)
// [1, 2] = 2212 0001 1110 0010 (in big-endian)
// final result = 2222 0000 1110 0001 0010 (big-endian, before compact)

int main(int argc, char **argv) {
	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");

	std::string indexfile = tempdir + "/tmpdata.query.index";
	std::string datametafile = tempdir + "/tmpdata.meta.index";

	for (auto test_it = TEST_CASES.cbegin(); test_it != TEST_CASES.cend(); test_it++) {
		const TestCase &test = *test_it;
		boost::shared_ptr< RegionEncoding > nondense_oracle = do_test< CBLQRegionEncoder<2>, CBLQSetOperations<2> >(test, "basic", CBLQRegionEncoderConfig(false), CBLQSetOperationsConfig(true));
		boost::shared_ptr< RegionEncoding > nary3 = do_test< CBLQRegionEncoder<2>, CBLQSetOperationsNAry3Dense<2> >(test, "nary3", CBLQRegionEncoderConfig(false), CBLQSetOperationsConfig(true));
		boost::shared_ptr< RegionEncoding > fast = do_test< CBLQRegionEncoder<2>, CBLQSetOperationsFast<2> >(test, "fast", CBLQRegionEncoderConfig(false), CBLQSetOperationsConfig(true));
		boost::shared_ptr< RegionEncoding > naryfast = do_test< CBLQRegionEncoder<2>, CBLQSetOperationsNAry3Fast<2> >(test, "nary3fast", CBLQRegionEncoderConfig(false), CBLQSetOperationsConfig(true));
		assert(*nary3 == *nondense_oracle);
		assert(*fast == *nondense_oracle);
		assert(*naryfast == *nondense_oracle);

		boost::shared_ptr< RegionEncoding > dense_oracle = do_test< CBLQRegionEncoder<2>, CBLQSetOperations<2> >(test, "basic-dense", CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true));
		boost::shared_ptr< RegionEncoding > dense_nary3 = do_test< CBLQRegionEncoder<2>, CBLQSetOperationsNAry3Dense<2> >(test, "nary3-dense", CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true));
		boost::shared_ptr< RegionEncoding > dense_fast = do_test< CBLQRegionEncoder<2>, CBLQSetOperationsFast<2> >(test, "fast-dense", CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true));
		boost::shared_ptr< RegionEncoding > dense_naryfast = do_test< CBLQRegionEncoder<2>, CBLQSetOperationsNAry3Fast<2> >(test, "nary3fast-dense", CBLQRegionEncoderConfig(true), CBLQSetOperationsConfig(true));
		assert(*dense_nary3 == *dense_oracle);
		assert(*dense_fast == *dense_oracle);
		assert(*dense_naryfast == *dense_oracle);
	}
}




