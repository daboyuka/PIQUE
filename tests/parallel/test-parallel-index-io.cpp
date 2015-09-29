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
 * test-index-parallel-io.cpp
 *
 *  Created on: Sep 4, 2014
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

#include <mpi.h>

#include "pique/data/grid.hpp"
#include "pique/data/dataset.hpp"
#include "pique/region/region-encoding.hpp"
#include "pique/indexing/binned-index.hpp"
#include "pique/indexing/index-builder.hpp"

#include "pique/region/ii/ii.hpp"
#include "pique/region/ii/ii-encode.hpp"

#include "pique/io/index-io.hpp"
#include "pique/io/posix/posix-index-io.hpp"
#include "pique/parallel/io/mpi-index-io.hpp"

#include "pique/util/universal-value.hpp"

#include "make-index.hpp"
#include "write-and-verify-index.hpp"
#include "standard-datasets.hpp"

template<typename RegionEncoderT>
void do_test(typename RegionEncoderT::RegionEncoderConfig conf, const std::vector< int > &domain, std::string indexfile) {
	boost::shared_ptr< BinnedIndex > idx = make_index< RegionEncoderT, int >(conf, domain);

	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	const IndexIOTypes::domain_offset_t domain_offset = rank * domain.size();

	write_and_verify_index< MPIIndexIO, POSIXIndexIO >(idx, indexfile, domain_offset, size);
}

int main(int argc, char **argv) {
	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");

	std::string tempfile = tempdir + "/test-parallel-index-io.idx";

	MPI_Init(&argc, &argv);

	do_test< IIRegionEncoder >(IIRegionEncoderConfig(), SMALL_DOMAIN, tempfile);

	srand(12345);
	std::vector<int> big_domain = make_big_domain(1ULL << 14, 30);

	do_test< IIRegionEncoder >(IIRegionEncoderConfig(), big_domain, tempfile);

	MPI_Finalize();
}




