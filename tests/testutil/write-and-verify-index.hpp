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
 * write-and-verify-index.hpp
 *
 *  Created on: Sep 8, 2014
 *      Author: David A. Boyuka II
 */
#ifndef WRITE_AND_VERIFY_INDEX_HPP_
#define WRITE_AND_VERIFY_INDEX_HPP_

#include <cassert>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/iterator/counting_iterator.hpp>

#include "pique/indexing/binning-spec.hpp"
#include "pique/indexing/binned-index.hpp"
#include "pique/io/index-io.hpp"

template<typename OutIndexIOT = POSIXIndexIO>
static void write_index(boost::shared_ptr< BinnedIndex > index, std::string indexfile, IndexIOTypes::domain_offset_t domain_offset = 0) {
	// Write the index to file
	OutIndexIOT outindexio;
	assert(outindexio.open(indexfile, IndexOpenMode::WRITE));

	boost::shared_ptr< IndexPartitionIO > partio = outindexio.append_partition();
	assert(partio != nullptr);

	partio->set_domain_global_offset(domain_offset);
	assert(partio->write_index(*index));
	assert(partio->close());

	assert(outindexio.close());
}

template<typename InIndexIOT = POSIXIndexIO>
static void verify_index(boost::shared_ptr< BinnedIndex > index, std::string indexfile, IndexIOTypes::domain_offset_t domain_offset = 0, IndexIOTypes::partition_count_t expected_partitions = 1) {
	using QKeyType = typename SigbitsBinningSpecification< int >::QKeyType;
	using region_id_t = BinnedIndexTypes::region_id_t;

	InIndexIOT inindexio;

	// Read the index back from file, piece by piece to test subset reads
	assert(inindexio.open(indexfile, IndexOpenMode::READ));
	assert(inindexio.get_num_partitions() == expected_partitions);

	boost::optional< IndexIOTypes::partition_id_t > part_id = inindexio.get_partition_with_domain_offset(domain_offset);
	assert(part_id);

	boost::shared_ptr< IndexPartitionIO > partio = inindexio.get_partition(*part_id);
	assert(partio != nullptr);

	IndexPartitionIO::PartitionMetadata pmeta = partio->get_partition_metadata();

	assert(pmeta.indexed_datatype && *pmeta.indexed_datatype == Datatypes::get_datatypeid_by_typeindex(index->get_indexed_datatype()));
	assert(pmeta.index_enc && *pmeta.index_enc == *index->get_encoding());
	assert(pmeta.index_rep && *pmeta.index_rep == index->get_representation_type());
	assert(pmeta.domain);
	assert(pmeta.domain->first == domain_offset);
	assert(pmeta.domain->second == index->get_domain_size());
	assert(pmeta.binning_spec && typeid(*pmeta.binning_spec) == typeid(*index->get_binning_specification()));
	assert(partio->get_num_bins() == index->get_num_bins());

	const std::vector< UniversalValue > expected_bin_keys = index->get_all_bin_keys();
	const std::vector< UniversalValue > actual_bin_keys = partio->get_all_bin_keys();
	assert(actual_bin_keys == expected_bin_keys);

	std::map< region_id_t, boost::shared_ptr< RegionEncoding > > regions;

	// Read all regions and check them
	partio->read_regions(std::set<BinnedIndexTypes::region_id_t>(
							boost::counting_iterator< region_id_t >(0),
							boost::counting_iterator< region_id_t >(index->get_num_regions())),
						regions);
	assert(regions.size() == index->get_num_regions());
	for (region_id_t i = 0; i < index->get_num_regions(); i++)
		assert(*regions[i] == *index->get_region(i));

	assert(partio->close());
	assert(inindexio.close());
}

template<typename OutIndexIOT = POSIXIndexIO, typename InIndexIOT = POSIXIndexIO>
static void write_and_verify_index(boost::shared_ptr< BinnedIndex > index, std::string indexfile, IndexIOTypes::domain_offset_t domain_offset = 0, IndexIOTypes::partition_count_t expected_partitions = 1) {
	write_index< OutIndexIOT >(index, indexfile, domain_offset);
	verify_index< InIndexIOT >(index, indexfile, domain_offset, expected_partitions);
}

#endif /* WRITE_AND_VERIFY_INDEX_HPP_ */
