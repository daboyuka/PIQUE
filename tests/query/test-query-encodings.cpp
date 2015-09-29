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
 * test-query-encodings.cpp
 *
 *  Created on: Dec 8, 2014
 *      Author: David A. Boyuka II
 */

#include <cstdint>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/data/grid.hpp"
#include "pique/data/dataset.hpp"
#include "pique/region/region-encoding.hpp"
#include "pique/indexing/binned-index.hpp"
#include "pique/indexing/index-builder.hpp"
#include "pique/setops/setops.hpp"

#include "pique/region/bitmap/bitmap.hpp"
#include "pique/region/bitmap/bitmap-encode.hpp"
#include "pique/setops/bitmap/bitmap-setops.hpp"

#include "pique/io/index-io.hpp"
#include "pique/io/posix/posix-index-io.hpp"

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
run_query(boost::shared_ptr< Database > db, boost::shared_ptr< AbstractSetOperations > setops, std::pair<int, int> bin_range, QueryEngine::QueryStats &qinfo) {
	BasicQueryEngine qe(setops);
	SimpleQueryEngine &qe2 = qe;
	Query q;

	q.push_back(boost::make_shared< ConstraintTerm >("var", UniversalValue(bin_range.first), UniversalValue(bin_range.second)));

	qe.open(db);
	boost::shared_ptr< RegionEncoding > result = qe2.evaluate(q, 0, qinfo);
	qe.close();

	return result;
}

struct TestCase {
	TestCase(const std::string testname, const std::vector<int> &domain, std::pair< int, int > bins_to_test) :
		testname(testname), domain(domain), bins_to_test(bins_to_test)
	{}

	const std::string testname;
	const std::vector<int> &domain;
	const std::pair< int, int > bins_to_test;
};

template<typename RegionEncoderT, typename SetOperationsT>
static boost::shared_ptr< Database >
prepare_test(
	const TestCase &test,
	typename RegionEncoderT::RegionEncoderConfig conf,
	IndexEncoding::Type enc_type,
	typename SetOperationsT::SetOperationsConfig setops_conf,
	std::string indexfile,
	std::string datametafile)
{
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
	return db;
}

static void do_test(
	const TestCase &test,
	std::vector< std::string > index_names,
	std::vector< boost::shared_ptr< Database > > dbs,
	boost::shared_ptr< AbstractSetOperations > setops)
{
	QueryEngine::QueryStats qinfo;
	for (int lb = test.bins_to_test.first; lb < test.bins_to_test.second; ++lb) {
		for (int ub = lb + 1; ub < test.bins_to_test.second; ++ub) {
			const std::pair< int, int > bin_range{lb, ub};

			auto db_it = dbs.begin(), db_end_it = dbs.end();
			auto index_name_it = index_names.begin();

			const std::string oracle_name = *index_name_it++;
			const boost::shared_ptr< RegionEncoding > oracle_region = run_query(*db_it++, setops, bin_range, qinfo);

			while (db_it != db_end_it) {
				const std::string index_name = *index_name_it++;
				boost::shared_ptr< RegionEncoding > result_region = run_query(*db_it++, setops, bin_range, qinfo);

				if (*result_region != *oracle_region) {
					std::cerr << "Error: for query [" << lb << ", " << (ub+1) << "), index \"" << index_name << "\" result doesn't match oracle index \"" << oracle_name << "\"" << std::endl;
					abort();
				}
			}
		}
	}
}

const std::vector< TestCase > TEST_CASES = {
	TestCase("small", SMALL_DOMAIN, {0, 3}),
	TestCase("medium", MEDIUM_DOMAIN, {0, 6}),
	TestCase("big", BIG_DOMAIN, {0, 10}),
	TestCase("huge", MANYBINS_DOMAIN, {0, 100}),
};

int main(int argc, char **argv) {
	using EncType = IndexEncoding::Type;

	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");

	std::string indexfile_prefix = tempdir + "/test-query-encodings.";
	std::string indexfile_suffix = tempdir + ".index";
	std::string datametafile = tempdir + "/test-query-encodings.meta";

	const std::vector< std::pair< std::string, EncType > > encs = {
		{ "equality", EncType::EQUALITY },
		{ "range", EncType::RANGE },
		{ "interval", EncType::INTERVAL },
		{ "hier", EncType::HIERARCHICAL },
		{ "binarycomp", EncType::BINARY_COMPONENT },
	};

	for (const TestCase &test : TEST_CASES) {
		std::vector< boost::shared_ptr< Database > > dbs;
		std::vector< std::string > index_filenames;
		boost::shared_ptr< BitmapSetOperations > setops = boost::make_shared< BitmapSetOperations >(BitmapSetOperationsConfig());

		for (auto enc : encs) {
			const std::string enc_name = enc.first;
			const EncType enc_type = enc.second;
			index_filenames.push_back(indexfile_prefix + enc_name + indexfile_suffix);
			dbs.push_back(prepare_test< BitmapRegionEncoder, BitmapSetOperations >(test, BitmapRegionEncoderConfig(), enc_type, BitmapSetOperationsConfig(), index_filenames.back(), datametafile));
		}

		do_test(test, index_filenames, dbs, setops);
	}
}




