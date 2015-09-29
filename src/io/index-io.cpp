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
 * index-io.cpp
 *
 *  Created on: May 6, 2014
 *      Author: David A. Boyuka II
 */

#include <cstdint>

#include "pique/io/index-io.hpp"

bool IndexIO::open_impl(std::string path, IndexOpenMode openmode) {
	this->openmode = openmode;
	return true;
}

bool IndexIO::close_impl() {
	this->openmode = IndexOpenMode::NOT_OPEN;
	return true;
}

boost::shared_ptr< IndexPartitionIO > IndexIO::get_partition(partition_id_t partition) {
	assert(this->openmode == IndexOpenMode::READ);

	boost::shared_ptr< IndexPartitionIO > part = get_partition_impl(partition);
	part->open(this->openmode);
	return part;
}

auto IndexIO::get_sorted_partition_domain_mappings() -> std::vector< domain_mapping_t > {
	const GlobalMetadata gmeta = get_global_metadata();

	std::vector< domain_mapping_t > out;
	for (partition_id_t p : gmeta.partitions) {
		const GlobalPartitionMetadata gpmeta = get_partition_metadata(p);
		out.push_back(std::make_pair(p, gpmeta.domain));
	}

	// Sort by ascending domain offset
	std::sort(out.begin(), out.end(),
			[](const domain_mapping_t &p1,
			   const domain_mapping_t &p2)
			   { return p1.second.first < p2.second.first; });

	return out;
}

auto IndexIO::get_partition_with_domain_offset(domain_offset_t global_domain_offset) -> boost::optional< partition_id_t > {
	const GlobalMetadata gmeta = get_global_metadata();
	for (partition_id_t pid : gmeta.partitions) {
		const GlobalPartitionMetadata gpmeta = get_partition_metadata(pid);
		if (gpmeta.domain.first == global_domain_offset)
			return boost::optional< partition_id_t >(pid);
	}
	return boost::none;
}

boost::shared_ptr< IndexPartitionIO > IndexIO::append_partition() {
	assert(this->openmode == IndexOpenMode::WRITE);

	boost::shared_ptr< IndexPartitionIO > part = append_partition_impl();
	part->open(this->openmode);
	return part;
}



bool IndexPartitionIO::open_impl(IndexOpenMode openmode) {
	this->openmode = openmode;
	if (openmode == IndexOpenMode::READ) assert(this->partition_id); // Ensure we have a partition ID in read mode
	return true;
}

bool IndexPartitionIO::close_impl() {
	this->openmode = IndexOpenMode::NOT_OPEN;
	return true;
}

uint64_t IndexPartitionIO::compute_regions_size(region_id_t lb, region_id_t ub) {
	if (lb > ub || ub > this->get_num_regions()) {
		abort();
		return 0;
	} else if (lb == ub) {
		return 0;
	} else {
		return compute_regions_size_impl(lb, ub);
	}
}

bool IndexPartitionIO::read_regions(const std::set< region_id_t > &regions_to_read, std::map< region_id_t, boost::shared_ptr< RegionEncoding > > &regions) {
	// Validate all regions to read
	const region_count_t num_regions = get_num_regions();
	for (auto it = regions_to_read.cbegin(); it != regions_to_read.cend(); it++) {
		const region_id_t region = *it;
		if (region >= num_regions)
			return false;
	}

	// Read the missing bins, fail if this fails
	return read_regions_impl(regions_to_read, regions);
}

void IndexPartitionIO::set_domain_global_offset(domain_offset_t global_offset) {
	PartitionMetadata pmeta;
	pmeta.domain = domain_t(global_offset, 0);
	this->set_partition_metadata(pmeta);
}

bool IndexPartitionIO::write_regions(const std::vector< boost::shared_ptr< RegionEncoding > > &regions) {
	assert(this->openmode == IndexOpenMode::WRITE);
	return this->write_regions_impl(regions);
}

bool IndexPartitionIO::write_index(BinnedIndex &index) {
	assert(this->openmode == IndexOpenMode::WRITE);

	PartitionMetadata pmeta = get_partition_metadata();

	const std::type_index datatype = index.get_indexed_datatype();
	const Datatypes::IndexableDatatypeID datatypeid = *Datatypes::get_datatypeid_by_typeindex(datatype); // Assume the datatype is valid, since it's coming from an AbstractBinnedIndex

	// Use the existing domain offset if present
	const uint64_t domain_global_offset = (pmeta.domain ? pmeta.domain->first : (domain_offset_t)0);

	std::vector< boost::shared_ptr< RegionEncoding > > regions;
	index.get_regions(regions);

	pmeta.indexed_datatype = datatypeid;
	pmeta.domain = std::make_pair(domain_global_offset, index.get_domain_size());
	pmeta.index_enc = index.get_encoding();
	pmeta.index_rep = index.get_representation_type();
	pmeta.binning_spec = index.get_binning_specification();

	set_partition_metadata(pmeta);
	return write_regions(regions);
}
