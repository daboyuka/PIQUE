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
 * test-index-io-cache.cpp
 *
 *  Created on: Jan 8, 2015
 *      Author: David A. Boyuka II
 */

#include <cstdint>
#include <cassert>
#include <cstdlib>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/indexing/binned-index.hpp"
#include "pique/region/ii/ii.hpp"
#include "pique/region/ii/ii-encode.hpp"
#include "pique/io/index-io.hpp"
#include "pique/io/posix/posix-index-io.hpp"
#include "pique/io/index-io-cache.hpp"

#include "make-index.hpp"

const std::vector<int> SMALL_DOMAIN = {0, 0, 0, 2, 1, 1, 1, 0, 2, 2, 2, 1, 0, 0, 1, 0};

void append_part(IndexIO &iio, BinnedIndex &index, IndexIOTypes::domain_offset_t domain_offset = 0) {
	boost::shared_ptr< IndexPartitionIO > ipio = iio.append_partition();
	ipio->set_domain_global_offset(domain_offset);
	ipio->write_index(index);
}

void create_test_database(std::string dir, std::string dbfile) {
	const std::string index1path = dir + "/index1.idx";
	const std::string index2path = dir + "/index2.idx";

	boost::shared_ptr< BinnedIndex > index = make_index< IIRegionEncoder, int >(IIRegionEncoderConfig(), SMALL_DOMAIN);

	boost::shared_ptr< IndexIO > iio = boost::make_shared< POSIXIndexIO >();
	iio->open(index1path, IndexOpenMode::WRITE);
	append_part(*iio, *index, 0*SMALL_DOMAIN.size());
	append_part(*iio, *index, 1*SMALL_DOMAIN.size());
	append_part(*iio, *index, 2*SMALL_DOMAIN.size());
	iio->close();
	iio->open(index2path, IndexOpenMode::WRITE);
	append_part(*iio, *index, 0*SMALL_DOMAIN.size());
	append_part(*iio, *index, 1*SMALL_DOMAIN.size());
	iio->close();

	Database db;
	db.add_variable("var1", boost::none, boost::make_optional(index1path));
	db.add_variable("var2", boost::none, boost::make_optional(index2path));
	db.save_to_file(dbfile);
}

void test_index_cache(std::string dbfile) {
	using IIOPtr = boost::cache_ptr< IndexIO >;
	using IPIOPtr = boost::cache_ptr< IndexPartitionIO >;

	boost::shared_ptr< Database > db = Database::load_from_file(dbfile);
	IndexIOCache cache(db);

	IIOPtr index1iio = cache.open_index_io("var1");
	assert(!index1iio.expired() && index1iio.lock()->is_open());
	IIOPtr index1iio2 = cache.open_index_io("var1");
	assert(!index1iio2.expired() && index1iio2.lock()->is_open() && index1iio.lock() == index1iio2.lock());

	IIOPtr index2iio = cache.open_index_io("var2");
	assert(!index2iio.expired() && index2iio.lock()->is_open() && index2iio.lock() != index1iio.lock());

	cache.release_unused();
	index1iio.weaken(); index1iio2.weaken(); index2iio.weaken();
	assert(!index1iio.expired() && !index1iio2.expired() && !index2iio.expired());

	index1iio.strengthen();
	cache.release_unused();
	assert(!index1iio.expired() && !index1iio2.expired() && index2iio.expired());

	index1iio.release_unused();
	assert(!index1iio.expired() && !index1iio2.expired() && index2iio.expired());

	cache.release_all();
	assert(!index1iio.expired() && !index1iio2.expired() && index2iio.expired());

	index1iio.release_unused();
	assert(index1iio.expired() && index1iio2.expired() && index2iio.expired());

	index1iio = cache.open_index_io("var1");
	index2iio = cache.open_index_io("var2");
	IPIOPtr index1part1 = index1iio.lock()->get_partition(0);
	IPIOPtr index1part2 = index1iio.lock()->get_partition(1);
	IPIOPtr index2part1 = index2iio.lock()->get_partition(0);
	assert(!index1part1.expired() && !index1part2.expired() && !index2part1.expired() && index1part1.lock() != index1part2.lock() && index1part1.lock() != index2part1.lock());

	index1part2.weaken();
	cache.release_unused();
	assert(!index1part1.expired() && index1part2.expired() && !index2part1.expired());

	index1part1.weaken();
	index1iio.weaken();
	cache.release_unused();
	assert(index1iio.expired() && index1part1.expired() && index1part2.expired() && !index2part1.expired());

	index2part1.weaken();
	cache.release_unused();
	assert(index1part1.expired() && index1part2.expired() && index2part1.expired());

	index2iio.weaken();
	cache.release_unused();
	assert(index2iio.expired());
}

int main(int argc, char **argv) {
	const char *tempdir_cstr = std::getenv("testworkdir");
	const std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");
	const std::string dbfile = tempdir + "/test.db";

	create_test_database(tempdir, dbfile);
	test_index_cache(dbfile);
}




