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
 * parallel-index-generator-impl.hpp
 *
 *  Created on: Sep 9, 2014
 *      Author: David A. Boyuka II
 */
#ifndef PARALLEL_INDEX_GENERATOR_IMPL_HPP_
#define PARALLEL_INDEX_GENERATOR_IMPL_HPP_

#include <algorithm>
#include "pique/parallel/indexing/parallel-index-generator.hpp"

template<typename datatype_t, typename RegionEncoderT, typename BinningSpecificationT>
ParallelIndexGenerator<datatype_t, RegionEncoderT, BinningSpecificationT>::ParallelIndexGenerator(
	MPI_Comm comm, RegionEncoderConfigType conf, boost::shared_ptr< BinningSpecificationType > binning_spec,
	boost::shared_ptr< IndexEncoding > index_enc, boost::shared_ptr< AbstractSetOperations > encode_setops) :
		comm(MPI_COMM_NULL), rank(-1), size(-1), stats(),
		parallel_indexio(comm, MASTER_RANK), index_builder(std::move(conf), std::move(binning_spec)),
		index_enc(index_enc),
		encode_setops(encode_setops)
{
	MPI_Comm_dup(comm, &this->comm);
	MPI_Comm_rank(comm, &this->rank);
	MPI_Comm_size(comm, &this->size);
}

template<typename datatype_t, typename RegionEncoderT, typename BinningSpecificationT>
void
ParallelIndexGenerator<datatype_t, RegionEncoderT, BinningSpecificationT>::generate_index(std::string indexio_path, const Dataset& data, Dataset::dataset_length_t subdomain_partition_size, bool dedicated_master, uint64_t subdomain_offset, uint64_t subdomain_size, bool relativize_subdoamin)
{
	// Preliminary computations
	const Dataset::dataset_length_t full_domain_size = data.get_element_count();
	subdomain_size = std::min(subdomain_size, full_domain_size);

	assert(subdomain_offset <= full_domain_size - subdomain_size);

	const uint64_t num_partitions = (subdomain_size - 1) / subdomain_partition_size + 1;
	const int num_indexing_ranks = this->size - (dedicated_master ? 1 : 0);
	const int this_indexing_rank = this->rank - (dedicated_master && this->rank > MASTER_RANK ? 1 : 0); // Rank among indexing ranks (remove the master rank and shift all higher ranks down by 1)

	TIME_STATS_TIME_BEGIN(stats.total_time)

	// Open the indexio
	this->parallel_indexio.open(indexio_path, IndexOpenMode::WRITE);

	if (dedicated_master && this->rank == MASTER_RANK) {
		// If we are the dedicated master process, close the index immediately.
		// This will force dedicated MPI message waiting until all other ranks close.
		this->parallel_indexio.close();
	} else {
		// In any other case, we are an indexing process, so index stuff
		for (uint64_t partnum = this_indexing_rank; partnum < num_partitions; partnum += num_indexing_ranks) {
			// Compute domain bounds
			const Dataset::dataset_offset_t part_domain_offset = subdomain_offset + partnum * subdomain_partition_size;
			const Dataset::dataset_offset_t part_domain_end_offset = std::min(part_domain_offset + subdomain_partition_size, subdomain_size);
			const Dataset::dataset_length_t part_domain_size = part_domain_end_offset - part_domain_offset;

			// Build the index partition from the subset of the dataset computed above
			const GridSubset subset(data.get_grid(), part_domain_offset, part_domain_size);
			boost::shared_ptr< BinnedIndex > part_index = this->index_builder.build_index(data, subset);

			if (this->index_enc) {
				assert(this->encode_setops);
				part_index = IndexEncoding::get_encoded_index(this->index_enc, part_index, *this->encode_setops);
			}

			// Write this partition to parallel_indexio
			boost::shared_ptr< IndexPartitionIO > partio = this->parallel_indexio.append_partition();
			partio->set_domain_global_offset(part_domain_offset - (relativize_subdoamin ? subdomain_offset : 0));
			partio->write_index(*part_index);
			partio->close();

			// Update stats
			stats.num_elements_indexed += part_domain_size;
			++stats.num_partitions_indexed;
			stats.indexio_stats += partio->get_io_stats();
		}

		// Close the indexio to finalize the file
		this->parallel_indexio.close();
	}

	TIME_STATS_TIME_END()

	stats.indexing_stats += this->index_builder.get_stats();
	stats.mpistats += this->parallel_indexio.get_stats();
}

#endif /* PARALLEL_INDEX_GENERATOR_IMPL_HPP_ */
