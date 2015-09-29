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
 * test-query-engines.cpp
 *
 *  Created on: Aug 8, 2014
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
#include "pique/setops/setops.hpp"

#include "pique/region/ii/ii.hpp"
#include "pique/region/ii/ii-encode.hpp"
#include "pique/region/cii/cii.hpp"
#include "pique/region/cii/cii-encode.hpp"
#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-encode.hpp"
#include "pique/region/wah/wah.hpp"
#include "pique/region/wah/wah-encode.hpp"
#include "pique/region/bitmap/bitmap.hpp"
#include "pique/region/bitmap/bitmap-encode.hpp"

#include "pique/setops/ii/ii-setops.hpp"
#include "pique/setops/cii/cii-setops.hpp"
#include "pique/setops/cblq/cblq-setops.hpp"
#include "pique/setops/wah/wah-setops.hpp"
#include "pique/setops/bitmap/bitmap-setops.hpp"

#include "pique/convert/region-convert.hpp"
#include "pique/convert/cblq/cblq-to-bitmap-convert.hpp"

#include "pique/io/index-io.hpp"
#include "pique/io/posix/posix-index-io.hpp"

#include "pique/query/query-engine.hpp"
#include "pique/query/basic-query-engine.hpp"
#include "pique/query/bitmap-query-engine.hpp"

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
run_query(boost::shared_ptr< QueryEngine > qe, boost::shared_ptr< Database > db, const std::vector< std::pair<int, int> > &bin_ranges, NArySetOperation combine_op, QueryEngine::QueryStats &qinfo) {
	Query q;

	for (auto it = bin_ranges.cbegin(); it != bin_ranges.cend(); it++)
		q.push_back(boost::make_shared< ConstraintTerm >("var", UniversalValue(it->first), UniversalValue(it->second)));
	q.push_back(boost::make_shared< NAryOperatorTerm >(combine_op, bin_ranges.size()));

	qe->open(db);
	boost::shared_ptr< RegionEncoding > result = qe->evaluate(q, 0, qinfo);
	qe->close();

	return result;
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

template<typename SetOperationsT>
static boost::shared_ptr< QueryEngine > make_basic_query_engine(typename SetOperationsT::SetOperationsConfig setops_conf) {
	return boost::make_shared< BasicQueryEngine >(boost::make_shared< SetOperationsT >(setops_conf));
}

template<typename BitmapConverterT, typename BitmapSetOperationsT>
static boost::shared_ptr< QueryEngine > make_bitmap_query_engine(typename BitmapSetOperationsT::SetOperationsConfig setops_conf) {
	return boost::make_shared< BitmapQueryEngine >(boost::make_shared< BitmapConverterT >(), boost::make_shared< BitmapSetOperationsT >(setops_conf));
}

template<typename RegionEncoderT, typename BinMergeSetOperationsT>
static void do_test(const TestCase &test, typename RegionEncoderT::RegionEncoderConfig conf, IndexEncoding::Type enc_type, typename BinMergeSetOperationsT::SetOperationsConfig binmerge_setops_conf, std::map< std::string, boost::shared_ptr< QueryEngine > > qes, std::string indexfile, std::string datametafile) {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;

	boost::shared_ptr< InMemoryDataset<int> > dataset = make_dataset(test.domain);
	boost::shared_ptr< BinnedIndex > flat_index = make_index< RegionEncoderT, int >(conf, dataset);

	boost::shared_ptr< BinMergeSetOperationsT > binmerge_setops = boost::make_shared< BinMergeSetOperationsT >(binmerge_setops_conf);

	boost::shared_ptr< BinnedIndex > index;
	if (enc_type == IndexEncoding::Type::EQUALITY)
		index = flat_index;
	else
		index = IndexEncoding::get_encoded_index(IndexEncoding::get_instance(enc_type), flat_index, *binmerge_setops);

	write_dataset_metadata_file(*dataset, datametafile);
	write_and_verify_index(index, indexfile);

	boost::shared_ptr< Database > db = make_database(indexfile, datametafile);

	std::map< std::string, boost::shared_ptr< RegionEncoding > > query_results;

	for (auto qe_it = qes.begin(); qe_it != qes.end(); ++qe_it) {
		QueryEngine::QueryStats qinfo;
		std::string qe_name = qe_it->first;
		boost::shared_ptr< QueryEngine > qe = qe_it->second;
		boost::shared_ptr< RegionEncoding > query_result = run_query(qe, db, test.bin_ranges, test.query_combine_op, qinfo);

		query_results[qe_name] = query_result;

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
		std::cout << "[STATS] Test \"" << test.testname << "\", query engine \"" << qe_name << "\": " <<
						"read=" << total_binread << ", " <<
						"binmerge=" << total_binmerge << ", " <<
						"termops=" << total_termops << std::endl;
#endif
	}

	std::string baseline_name;
	std::vector< uint32_t > baseline_rids, check_rids;
	baseline_name = query_results.begin()->first;
	query_results.begin()->second->convert_to_rids(baseline_rids, true, false);
	for (auto result_it = ++query_results.begin(); result_it != query_results.end(); ++result_it) {
		std::string check_name = result_it->first;
		result_it->second->convert_to_rids(check_rids, true, false);

		if (baseline_rids != check_rids) {
			std::cerr << "Results from query engine "
					<< "\"" << baseline_name << "\" (" << baseline_rids.size() << " RIDs) and "
					<< "\"" << check_name << "\" (" << check_rids.size() << " RIDs) do not match" << std::endl;
			abort();
		}
	}

#ifdef VERBOSE_TESTS
	std::cout << "[PASS] Test \"" << test.testname << "\" with index rep. \"" << typeid(RegionEncoderT).name() << "\" and index enc. " << (int)enc_type << std::endl;
#endif
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

	TestCase("empty-operand-union", SMALL_DOMAIN, std::vector< std::pair<int, int> >{{0, 0}, {3, 3}}, NArySetOperation::UNION),
	TestCase("empty-operand-inter", SMALL_DOMAIN, std::vector< std::pair<int, int> >{{0, 0}, {3, 3}}, NArySetOperation::INTERSECTION),
	TestCase("filled-operand-union", SMALL_DOMAIN, std::vector< std::pair<int, int> >{{0, 0}, {0, 2}}, NArySetOperation::UNION),
	TestCase("filled-operand-inter", SMALL_DOMAIN, std::vector< std::pair<int, int> >{{0, 0}, {0, 2}}, NArySetOperation::INTERSECTION),
};

int main(int argc, char **argv) {
	using EncType = IndexEncoding::Type;

	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");

	std::string indexfile = tempdir + "/tmpdata.query.index";
	std::string datametafile = tempdir + "/tmpdata.meta.index";

	for (auto test_it = TEST_CASES.cbegin(); test_it != TEST_CASES.cend(); test_it++) {
		const TestCase &test = *test_it;
		//do_test< IIRegionEncoder, IISetOperations, IISetOperations >(test, IIRegionEncoderConfig(), EncType::FLAT, IISetOperationsConfig(), IISetOperationsConfig(), indexfile, datametafile);
		//do_test< CIIRegionEncoder, CIISetOperationsNAry, CIISetOperations >(test, CIIRegionEncoderConfig(), EncType::FLAT, CIISetOperationsConfig(), CIISetOperationsConfig(), indexfile, datametafile);

		std::map< std::string, boost::shared_ptr< QueryEngine > > qes;
		qes["basic"] = make_basic_query_engine< CBLQSetOperationsNAry3Fast<2> >(CBLQSetOperationsConfig(true));
		qes["bitmap"] = make_bitmap_query_engine< CBLQToBitmapConverter<2>, BitmapSetOperations >(BitmapSetOperationsConfig());
		qes["bitmap-df"] = make_bitmap_query_engine< CBLQToBitmapDFConverter<2>, BitmapSetOperations >(BitmapSetOperationsConfig());

		std::vector< EncType > encs = {  EncType::EQUALITY, EncType::RANGE, EncType::INTERVAL, EncType::HIERARCHICAL, EncType::BINARY_COMPONENT, };
		for (auto it = encs.begin(); it != encs.end(); ++it) {
			const EncType enc_type = *it;
			//do_test< WAHRegionEncoder, WAHSetOperations, WAHSetOperations >(test, WAHRegionEncoderConfig(), enc_type, WAHSetOperationsConfig(), WAHSetOperationsConfig(), indexfile, datametafile);
			do_test< CBLQRegionEncoder<2>, CBLQSetOperationsNAry3Fast<2> >(test, CBLQRegionEncoderConfig(false), enc_type, CBLQSetOperationsConfig(true), qes, indexfile, datametafile);
		}
	}
}




