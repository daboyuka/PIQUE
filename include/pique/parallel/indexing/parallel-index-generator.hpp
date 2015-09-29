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
 * parallel-index-generator.hpp
 *
 *  Created on: Sep 9, 2014
 *      Author: David A. Boyuka II
 */
#ifndef PARALLEL_INDEX_GENERATOR_HPP_
#define PARALLEL_INDEX_GENERATOR_HPP_

#include <string>
#include <boost/smart_ptr.hpp>
#include <boost/optional.hpp>

#include "pique/indexing/binned-index.hpp"
#include "pique/indexing/index-builder.hpp"
#include "pique/encoding/index-encoding.hpp"
#include "pique/io/index-io.hpp"
#include "pique/parallel/io/mpi-index-io.hpp"
#include "pique/setops/setops.hpp"
#include "pique/stats/stats.hpp"

#include <mpi.h>

struct ParallelIndexingStats {
	IndexIOTypes::partition_count_t num_partitions_indexed{0};
	Dataset::dataset_length_t num_elements_indexed{0};

	TimeStats total_time;
	IndexBuilderStats indexing_stats;
	IOStats indexio_stats;
	MPIIndexIOStats mpistats;
};

DEFINE_STATS_SERIALIZE(ParallelIndexingStats, stats, \
	stats.num_partitions_indexed & stats.num_elements_indexed & \
	stats.total_time & stats.indexing_stats & stats.indexio_stats & stats.mpistats);

template<typename datatype_t, typename RegionEncoderT, typename BinningSpecificationT>
class ParallelIndexGenerator {
public:
	using RegionEncoderType = RegionEncoderT;
	using RegionEncoderConfigType = typename RegionEncoderType::RegionEncoderConfig;
	using BinningSpecificationType = BinningSpecificationT;
	using RegionEncodingType = typename RegionEncoderT::RegionEncodingOutT;
	using QuantizationType = typename BinningSpecificationT::QuantizationType;
	using QuantizedKeyCompareType = typename BinningSpecificationT::QuantizedKeyCompareType;

public:
	ParallelIndexGenerator(MPI_Comm comm, RegionEncoderConfigType conf, boost::shared_ptr< BinningSpecificationType > binning_spec,
			boost::shared_ptr< IndexEncoding > index_enc = nullptr, boost::shared_ptr< AbstractSetOperations > encode_setops = nullptr);

	/*
	 * indexio_path: where to store the index, as interpreted by MPIIndexIO
	 * data: the dataset to index
	 * domain_partition_size: the (maximum) number of elements per partition to index (all partitions will be of this size, except possibly the last)
	 * dedicated_master: if true, one MPI rank will not perform indexing, instead solely directing the allocation of partitions to other ranks for indexing
	 * subdomain_offset: the offset of the first element in "data" to index (default 0)
	 * subdomain_size: the maximum number of elements in "data" to index (default max uint64_t)
	 * relativize_subdomain: if true, the subdomain specified by subdomain_offset/subdomain_size is treated as the whole domain; that is, element
	 *                       "subdomain_offset" in "data" is considered RID 0; if false, RIDs correspond to positions in the full "data" dataset (default true)
	 *
	 * This function will index elements [subdomain_offset, subdomain_offset + subdomain_size) intersect [0, data.get_element_count()), which will
	 * correspond to RIDs [subdomain_offset, subdomain_offset + min(subdomain_size, data.get_element_count())) (if relativize_domain is true), or
	 * RIDs [0, min(subdomain_size, data.get_element_count())) (if relativize_domain is false).
	 */
    void generate_index(std::string indexio_path, const Dataset &data, Dataset::dataset_length_t domain_partition_size, bool dedicated_master = false,
    		uint64_t subdomain_offset = 0, uint64_t subdomain_size = std::numeric_limits<uint64_t>::max(), bool relativize_subdoamin = true);

    ParallelIndexingStats get_stats() const { return stats; }
    void reset_stats() { stats = ParallelIndexingStats(); }

private:
    static constexpr int MASTER_RANK = 0;
private:
	MPI_Comm comm;
	int rank, size;

	mutable ParallelIndexingStats stats;

	MPIIndexIO parallel_indexio;
    IndexBuilder< datatype_t, RegionEncoderType, BinningSpecificationType > index_builder;

    boost::shared_ptr< IndexEncoding > index_enc;
    boost::shared_ptr< AbstractSetOperations > encode_setops;
};

#include "impl/parallel-index-generator-impl.hpp"

#endif /* PARALLEL_INDEX_GENERATOR_HPP_ */
