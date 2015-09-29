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

#include "pique/util/variant-is-type.hpp"

#include "pique/data/grid.hpp"
#include "pique/data/dataset.hpp"
#include "pique/region/region-encoding.hpp"
#include "pique/indexing/binned-index.hpp"
#include "pique/indexing/index-builder.hpp"
#include "pique/setops/setops.hpp"

#include "pique/region/ii/ii.hpp"
#include "pique/region/ii/ii-encode.hpp"
#include "pique/region/cii/cii.hpp"
#include "pique/region/cii/cii-encode.hpp"
#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-encode.hpp"
#include "pique/region/wah/wah.hpp"
#include "pique/region/wah/wah-encode.hpp"

#include "pique/setops/ii/ii-setops.hpp"
#include "pique/setops/cii/cii-setops.hpp"
#include "pique/setops/cblq/cblq-setops.hpp"
#include "pique/setops/wah/wah-setops.hpp"

#include "pique/io/index-io.hpp"
#include "pique/io/posix/posix-index-io.hpp"

#include "pique/query/query-engine.hpp"
#include "pique/query/basic-query-engine.hpp"

#include "make-index.hpp"
#include "write-and-verify-index.hpp"
#include "write-dataset-metafile.hpp"
#include "standard-datasets.hpp"

static boost::shared_ptr< InMemoryDataset<int> >
make_dataset(const std::vector<int> &domain) {
	boost::shared_ptr< InMemoryDataset<int> > dataset = boost::make_shared< InMemoryDataset<int> >((std::vector<int>(domain)), Grid{domain.size()});

	return dataset;
}

static boost::shared_ptr< Database > make_database(std::string indexfile, std::string datametafile) {
	boost::shared_ptr< Database > db = boost::make_shared< Database >();
	boost::shared_ptr< DataVariable > var = boost::make_shared< DataVariable >("var", datametafile, indexfile);

	db->add_variable(var);
	assert(var->get_varid() == 0);
	assert(db->get_variable("var") == var);
	assert(db->get_variable(0) == var);

	return db;
}

static boost::shared_ptr< RegionEncoding >
run_query(boost::shared_ptr< Database > db, boost::shared_ptr< AbstractSetOperations > setops, const std::vector< std::pair<int, int> > &bin_ranges, NArySetOperation combine_op, QueryEngine::QueryStats &qstats) {
	BasicQueryEngine qe(setops);
	Query q;

	for (auto it = bin_ranges.cbegin(); it != bin_ranges.cend(); it++)
		q.push_back(boost::make_shared< ConstraintTerm >("var", UniversalValue(it->first), UniversalValue(it->second)));
	q.push_back(boost::make_shared< NAryOperatorTerm >(combine_op, bin_ranges.size()));

	qe.open(db);
	boost::shared_ptr< RegionEncoding > result = qe.evaluate(q, 0, qstats);
	qe.close();

	return result;
}

// Overly complex, and possibly broken
static void check_query_result_old(boost::shared_ptr< BinnedIndex > original_index, boost::shared_ptr< AbstractSetOperations > setops, const boost::shared_ptr< RegionEncoding > query_result, const std::vector< std::pair<int, int> > &bin_ranges, NArySetOperation combine_op) {
	using bin_id_t = BinnedIndexTypes::bin_id_t;
	using bin_count_t = BinnedIndexTypes::bin_count_t;
	using RU = RegionEncoding::RegionUniformity;

	boost::shared_ptr< RegionEncoding > expected_result = nullptr;
	std::vector< boost::shared_ptr< RegionEncoding > > bin_regions;
	std::vector< RegionEncoding::RegionUniformity > bin_region_unifs;

	for (auto bin_range_it = bin_ranges.cbegin(); bin_range_it != bin_ranges.cend(); bin_range_it++) {
		const std::pair<bin_id_t, bin_id_t> &bin_range = *bin_range_it;

		if (bin_range.first == 0 && bin_range.second == original_index->get_num_bins() + 1) {
			bin_regions.push_back(nullptr);
			bin_region_unifs.push_back(RU::FILLED);
		} else {
			boost::shared_ptr< RegionEncoding > bin_range_regions_merged = nullptr;
			for (bin_id_t i = bin_range.first; i <= bin_range.second; i++) {
				if (i >= original_index->get_num_bins())
					break;

				boost::shared_ptr< RegionEncoding > bin = original_index->get_region(i);
				if (bin_range_regions_merged)
					bin_range_regions_merged = setops->dynamic_binary_set_op(bin_range_regions_merged, bin, NArySetOperation::UNION);
				else
					bin_range_regions_merged = bin;
			}

			bin_regions.push_back(bin_range_regions_merged);
			bin_region_unifs.push_back(bin_range_regions_merged != nullptr ? RU::MIXED : RU::EMPTY);
		}
	}

	const SimplifiedSetOp simplified = SimplifiedSetOp::simplify(combine_op, bin_region_unifs);
	assert(simplified.result_uniformity == RU::MIXED);

	std::vector< boost::shared_ptr< RegionEncoding > > mixed_bin_regions;
	for (size_t i = 0; i < bin_regions.size(); ++i)
		if (bin_region_unifs[i] == RU::MIXED)
			mixed_bin_regions.push_back(bin_regions[i]);

	expected_result = setops->dynamic_nary_set_op(mixed_bin_regions.cbegin(), mixed_bin_regions.cend(), combine_op);
	if (simplified.complement_op_result)
		expected_result = setops->dynamic_unary_set_op(expected_result, UnarySetOperation::COMPLEMENT);

	assert(*expected_result == *query_result);
}

static void check_query_result(const boost::shared_ptr< RegionEncoding > query_result, const std::vector<int> &data, const std::vector< std::pair<int, int> > &bin_ranges, NArySetOperation combine_op) {
	std::vector< uint32_t > rids;
	query_result->convert_to_rids(rids, true, true);
	auto it = rids.cbegin(), end_it = rids.cend();

	for (size_t i = 0; i < data.size(); ++i) {
		const int value = data[i];

		bool is_rid_expected = (combine_op == NArySetOperation::UNION ? false : true);
		for (const std::pair<int, int> &range : bin_ranges) {
			const bool in_range = (value >= range.first && value <= range.second);

			if (combine_op == NArySetOperation::UNION)
				is_rid_expected |= in_range;
			else if (combine_op == NArySetOperation::INTERSECTION)
				is_rid_expected &= in_range;
		}

		if (is_rid_expected) {
			assert(it != end_it && *it == i);
			++it;
		} else {
			assert(it == end_it || *it != i);
		}
	}
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
static void do_test(const TestCase &test, typename RegionEncoderT::RegionEncoderConfig conf, IndexEncoding::Type enc_type, typename SetOperationsT::SetOperationsConfig setops_conf, std::string indexfile, std::string datametafile) {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;

	boost::shared_ptr< SetOperationsT > setops = boost::make_shared< SetOperationsT >(setops_conf);

	boost::shared_ptr< InMemoryDataset<int> > dataset = make_dataset(test.domain);
	boost::shared_ptr< BinnedIndex > flat_index = make_index< RegionEncoderT, int >(conf, dataset);

	boost::shared_ptr< BinnedIndex > index;
	if (enc_type == IndexEncoding::Type::EQUALITY)
		index = flat_index;
	else
		index = IndexEncoding::get_encoded_index(IndexEncoding::get_instance(enc_type), flat_index, *setops);

	write_dataset_metadata_file(*dataset, datametafile);
	write_and_verify_index(index, indexfile);

	boost::shared_ptr< Database > db = make_database(indexfile, datametafile);

	QueryEngine::QueryStats qinfo;
	boost::shared_ptr< RegionEncoding > query_result = run_query(db, setops, test.bin_ranges, test.query_combine_op, qinfo);

	check_query_result(query_result, test.domain, test.bin_ranges, test.query_combine_op);

	double total_binread = 0, total_binmerge = 0, total_termops = 0;
	for (auto it = qinfo.terminfos.begin(); it != qinfo.terminfos.end(); it++) {
		if (typeid(**it) == typeid(QueryEngine::ConstraintTermEvalStats)) {
			QueryEngine::ConstraintTermEvalStats &cterminfo = static_cast<QueryEngine::ConstraintTermEvalStats &>(**it);
			total_binread += cterminfo.binread.read_time;
			total_binmerge += cterminfo.binmerge.time;
		} else {
			QueryEngine::MultivarTermEvalStats &mterminfo = static_cast<QueryEngine::MultivarTermEvalStats &>(**it);
			total_termops += mterminfo.total.time;
		}
	}

#ifdef VERBOSE_TESTS
	std::cout << "[PASS] Test \"" << test.testname << "\" with index rep. \"" << typeid(RegionEncoderT).name() << "\" and index enc. " << (int)enc_type << std::endl;
	std::cout << "[STATS] Test \"" << test.testname << "\": " <<
					"read=" << total_binread << ", " <<
					"binmerge=" << total_binmerge << ", " <<
					"termops=" << total_termops << std::endl;
#endif
}

// Bin bounds are inclusive, since they represent a [lkey, hkey) range, and even an exclusive hkey implies an inclusive hkey bin read
const std::vector< TestCase > TEST_CASES = {
	TestCase("small-union", SMALL_DOMAIN, std::vector< std::pair<int, int> >{{0, 0}, {2, 2}}, NArySetOperation::UNION),
	TestCase("small-inter", SMALL_DOMAIN, std::vector< std::pair<int, int> >{{0, 1}, {1, 2}}, NArySetOperation::INTERSECTION),
	TestCase("med-union", MEDIUM_DOMAIN, std::vector< std::pair<int, int> >{{0, 0}, {2, 3}, {5, 5}}, NArySetOperation::UNION),
	TestCase("med-inter", MEDIUM_DOMAIN, std::vector< std::pair<int, int> >{{0, 3}, {2, 5}}, NArySetOperation::INTERSECTION),
	TestCase("big-inter", BIG_DOMAIN, std::vector< std::pair<int, int> >{{2, 6}, {5, 9}}, NArySetOperation::INTERSECTION),
};

int main(int argc, char **argv) {
	using EncType = IndexEncoding::Type;

	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");

	std::string indexfile = tempdir + "/tmpdata.query.index";
	std::string datametafile = tempdir + "/tmpdata.meta.index";

	for (auto test_it = TEST_CASES.cbegin(); test_it != TEST_CASES.cend(); test_it++) {
		const TestCase &test = *test_it;
		do_test< IIRegionEncoder, IISetOperations >(test, IIRegionEncoderConfig(), EncType::EQUALITY, IISetOperationsConfig(), indexfile, datametafile);
		do_test< CIIRegionEncoder, CIISetOperations >(test, CIIRegionEncoderConfig(), EncType::EQUALITY, CIISetOperationsConfig(), indexfile, datametafile);
		do_test< CIIRegionEncoder, CIISetOperationsNAry >(test, CIIRegionEncoderConfig(), EncType::EQUALITY, CIISetOperationsConfig(), indexfile, datametafile);

		std::vector< EncType > encs = {  EncType::EQUALITY, EncType::RANGE, EncType::INTERVAL, EncType::HIERARCHICAL, EncType::BINARY_COMPONENT, };
		for (auto it = encs.begin(); it != encs.end(); ++it) {
			const EncType enc_type = *it;
			do_test< WAHRegionEncoder, WAHSetOperations >(test, WAHRegionEncoderConfig(), enc_type, WAHSetOperationsConfig(), indexfile, datametafile);
			do_test< CBLQRegionEncoder<2>, CBLQSetOperationsFast<2> >(test, CBLQRegionEncoderConfig(false), enc_type, CBLQSetOperationsConfig(true), indexfile, datametafile);
			do_test< CBLQRegionEncoder<2>, CBLQSetOperationsFast<2> >(test, CBLQRegionEncoderConfig(true), enc_type, CBLQSetOperationsConfig(true), indexfile, datametafile);
			do_test< CBLQRegionEncoder<2>, CBLQSetOperationsNAry3Fast<2> >(test, CBLQRegionEncoderConfig(false), enc_type, CBLQSetOperationsConfig(true), indexfile, datametafile);
			do_test< CBLQRegionEncoder<2>, CBLQSetOperationsNAry3Fast<2> >(test, CBLQRegionEncoderConfig(true), enc_type, CBLQSetOperationsConfig(true), indexfile, datametafile);
		}
	}
}




