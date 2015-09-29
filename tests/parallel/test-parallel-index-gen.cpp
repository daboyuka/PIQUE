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
 * test-index-parallel-gen.cpp
 *
 *  Created on: Sep 10, 2014
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

#include "pique/parallel/indexing/parallel-index-generator.hpp"

#include "pique/util/universal-value.hpp"

#include "make-index.hpp"
#include "write-and-verify-index.hpp"
#include "standard-datasets.hpp"

static boost::shared_ptr< InMemoryDataset<int> >
make_dataset(const std::vector<int> &domain) {
	boost::shared_ptr< InMemoryDataset<int> > dataset = boost::make_shared< InMemoryDataset<int> >((std::vector<int>(domain)), Grid{domain.size()});

	return dataset;
}

template<typename RegionEncoderT>
void do_test(typename RegionEncoderT::RegionEncoderConfig conf, const std::vector< int > &domain, std::string indexfile, Dataset::dataset_length_t partition_domain_size) {
	using SigbitsBinningSpec = SigbitsBinningSpecification< int >;

	boost::shared_ptr< InMemoryDataset<int> > dataset = make_dataset(domain);

	{
		ParallelIndexGenerator< int, RegionEncoderT, SigbitsBinningSpec > pll_index_gen(MPI_COMM_WORLD, conf, boost::make_shared< SigbitsBinningSpec >(sizeof(int)*8));
		pll_index_gen.generate_index(indexfile, *dataset, partition_domain_size);
	}

	// Now, the master process will perform a serial version of the index build, and will verify all partitions
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	if (rank == 0) { // is master
		const uint64_t expected_partition_count = (domain.size() - 1) / partition_domain_size + 1;

		POSIXIndexIO serial_indexio;
		std::vector< boost::shared_ptr< BinnedIndex > > part_indexes;

		serial_indexio.open(indexfile + ".serialcompare", IndexOpenMode::WRITE);
		IndexBuilder< int, RegionEncoderT, SigbitsBinningSpec > index_builder(conf, boost::make_shared< SigbitsBinningSpec >(sizeof(int)*8));
		for (uint64_t partid = 0; partid < expected_partition_count; partid++) {
			const Dataset::dataset_offset_t this_part_domain_off = partid * partition_domain_size;
			const Dataset::dataset_offset_t this_part_domain_end_off = std::min(this_part_domain_off + partition_domain_size, domain.size());
			const Dataset::dataset_length_t this_part_domain_size = this_part_domain_end_off - this_part_domain_off;

			GridSubset subset(dataset->get_grid(), this_part_domain_off, this_part_domain_size);
			boost::shared_ptr< BinnedIndex > part_index = index_builder.build_index(*dataset, subset);

			boost::shared_ptr< IndexPartitionIO > partio = serial_indexio.append_partition();
			partio->set_domain_global_offset(this_part_domain_off);
			partio->write_index(*part_index);
			partio->close();

			part_indexes.push_back(part_index);
		}
		serial_indexio.close();

		for (uint64_t partid = 0; partid < expected_partition_count; partid++) {
			const Dataset::dataset_offset_t this_part_domain_off = partid * partition_domain_size;
			//const Dataset::dataset_offset_t this_part_domain_end_off = std::min(this_part_domain_off + partition_domain_size, domain.size());
			//const Dataset::dataset_length_t this_part_domain_size = this_part_domain_end_off - this_part_domain_off;

#ifdef VERBOSE_TESTS
			std::cerrr << "Checking partition " << (partid+1) << " of " << expected_partition_count << "..." << std::endl;
#endif
			verify_index< POSIXIndexIO >(part_indexes[partid], indexfile + ".serialcompare", this_part_domain_off, expected_partition_count);
		}
	}
}

int main(int argc, char **argv) {
	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");

	std::string tempfile = tempdir + "/test-parallel-index-gen.idx";

	MPI_Init(&argc, &argv);

	do_test< IIRegionEncoder >(IIRegionEncoderConfig(), SMALL_DOMAIN, tempfile, 4);

	srand(12345);
	std::vector<int> big_domain = make_big_domain(1ULL << 14, 30);

	do_test< IIRegionEncoder >(IIRegionEncoderConfig(), big_domain, tempfile, big_domain.size() / 4 / 4); // divide by 4 for expected MPI size, divide by 4 again for desired partitiosn per rank

	MPI_Finalize();
}




