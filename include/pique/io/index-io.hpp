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
 * index-io.hpp
 *
 *  Created on: May 6, 2014
 *      Author: David A. Boyuka II
 */
#ifndef INDEX_IO_HPP_
#define INDEX_IO_HPP_

#include <cstdint>
#include <string>
#include <vector>
#include <set>
#include <map>

#include "pique/indexing/binned-index.hpp"
#include "pique/io/index-io-types.hpp"
#include "pique/util/serializable-chunk.hpp"
#include "pique/util/datatypes.hpp"
#include "pique/util/universal-value.hpp"
#include "pique/util/openclose.hpp"
#include "pique/stats/stats.hpp"

enum struct IndexOpenMode { NOT_OPEN, READ, WRITE, WRITE_APPEND };

class IndexPartitionIO; // fwd decl.



class IndexIO : public OpenableCloseable< std::string, IndexOpenMode > {
public:
	using partition_id_t	= IndexIOTypes::partition_id_t;
	using partition_count_t	= IndexIOTypes::partition_count_t;
	using domain_offset_t	= IndexIOTypes::domain_offset_t;
	using domain_size_t		= IndexIOTypes::domain_size_t;
	using domain_t			= IndexIOTypes::domain_t;
	using domain_mapping_t	= IndexIOTypes::domain_mapping_t;

	// Metadata pertaining to the index file as a whole
	struct GlobalMetadata {
		std::vector< partition_id_t > partitions;
	};
	// Metadata pertaining to a particular index partition, but which is available without opening that partition
	struct GlobalPartitionMetadata {
		domain_t domain;
	};

public:
	IndexIO() : openmode(IndexOpenMode::NOT_OPEN) {}
	virtual ~IndexIO() { close(); }

	IndexOpenMode get_open_mode() const {
		if (!is_open()) return IndexOpenMode::NOT_OPEN;
		else			return openmode;
	}

	partition_count_t get_num_partitions() { return get_global_metadata().partitions.size(); }
	GlobalMetadata get_global_metadata() { return get_global_metadata_impl(); }
	GlobalPartitionMetadata get_partition_metadata(partition_id_t partition) { return get_partition_metadata_impl(partition, partition + 1)[0]; }
	std::vector< GlobalPartitionMetadata > get_all_partition_metadatas() { return get_partition_metadata_impl(0, (partition_id_t)get_num_partitions()); }

	std::vector< domain_mapping_t > get_sorted_partition_domain_mappings();
	boost::optional< partition_id_t > get_partition_with_domain_offset(domain_offset_t global_domain_offset);

	boost::shared_ptr< IndexPartitionIO > get_partition(partition_id_t partition);
	boost::shared_ptr< IndexPartitionIO > append_partition();

protected:
	// Always call up to these from derived classes
	virtual bool open_impl(std::string path, IndexOpenMode openmode);
	virtual bool close_impl();

private:
	virtual GlobalMetadata get_global_metadata_impl() = 0;
	virtual std::vector< GlobalPartitionMetadata > get_partition_metadata_impl(partition_id_t begin_partition, partition_id_t end_partition) = 0;

	virtual boost::shared_ptr< IndexPartitionIO > get_partition_impl(partition_id_t partition) = 0;
	virtual boost::shared_ptr< IndexPartitionIO > append_partition_impl() = 0;

private:
	IndexOpenMode openmode;
};



class IndexPartitionIO : public OpenableCloseable< IndexOpenMode > {
public:
	using bin_id_t			= BinnedIndexTypes::bin_id_t;
	using bin_count_t		= BinnedIndexTypes::bin_count_t;
	using region_id_t		= BinnedIndexTypes::region_id_t;
	using region_count_t	= BinnedIndexTypes::region_count_t;

	using partition_id_t	= IndexIO::partition_id_t;
	using partition_count_t	= IndexIO::partition_count_t;
	using domain_offset_t	= IndexIO::domain_offset_t;
	using domain_size_t		= IndexIO::domain_size_t;
	using domain_t			= IndexIO::domain_t;

protected:
	IndexPartitionIO(boost::optional< partition_id_t > partition_id) :
		openmode(IndexOpenMode::NOT_OPEN), partition_id(partition_id)
	{}

public:
	// Metadata pertaining to a particular index partition
	struct PartitionMetadata {
		using DTID = Datatypes::IndexableDatatypeID;
		using ABS = AbstractBinningSpecification;
		boost::optional< DTID					>	indexed_datatype;
		boost::optional< domain_t				>	domain;
		boost::optional< RegionEncoding::Type	>	index_rep;
		boost::shared_ptr< const IndexEncoding	>	index_enc;
		boost::shared_ptr< const ABS			>	binning_spec;

		bool is_filled() {
			return indexed_datatype && domain && index_rep && index_enc && binning_spec;
		}
		void init_empty() {
			indexed_datatype = Datatypes::IndexableDatatypeID::UNDEFINED;
			domain = domain_t();
			index_rep = RegionEncoding::Type::UNKNOWN;
			index_enc = nullptr;
			binning_spec = nullptr;
		}
		// Only updates values that are initialized in "other"
		void update(const PartitionMetadata &other) {
			if (other.indexed_datatype)	this->indexed_datatype = other.indexed_datatype;
			if (other.domain)			this->domain = other.domain;
			if (other.index_rep)		this->index_rep = other.index_rep;
			if (other.index_enc)		this->index_enc = other.index_enc;
			if (other.binning_spec)		this->binning_spec = other.binning_spec;
		}
	};

	virtual ~IndexPartitionIO() { close(); }

	IndexOpenMode get_open_mode() const {
		if (!is_open()) return IndexOpenMode::NOT_OPEN;
		else			return openmode;
	}

	// === IO stats API: may be called in any mode
	virtual IOStats get_io_stats() const = 0;
	virtual void reset_io_stats() = 0;

	// === Shared API: may be called any time in read mode, or after a corresponding set call in write mode
	PartitionMetadata get_partition_metadata() { return get_partition_metadata_impl(); } // In write mode, can be called after a set_partition_metadata() call
	region_count_t get_num_regions() { return get_num_regions_impl(); } // In write mode, can be called after a write_regions() call
	bin_count_t get_num_bins() { return get_partition_metadata().binning_spec->get_num_bins(); }
	UniversalValue get_bin_key(bin_id_t bin) { return get_partition_metadata().binning_spec->get_bin_key(bin); }
	std::vector< UniversalValue > get_all_bin_keys() { return get_partition_metadata().binning_spec->get_all_bin_keys(); }

	// Read API
	partition_id_t get_partition_id() const { return *partition_id; }
	uint64_t compute_regions_size(region_id_t lb, region_id_t ub); // [lb, ub)
	bool read_regions(const std::set< region_id_t > &regions_to_read, std::map< region_id_t, boost::shared_ptr< RegionEncoding > > &regions);

	// === Write API: may only be called in write mode
	void set_partition_metadata(PartitionMetadata metadata) { assert(get_open_mode() == IndexOpenMode::WRITE); set_partition_metadata_impl(metadata); } // Only sets those fields that boost::optional-initialized
	void set_domain_global_offset(domain_offset_t global_offset);
	bool write_regions(const std::vector< boost::shared_ptr< RegionEncoding > > &regions);
	bool write_index(BinnedIndex &index);

protected:
	// Always call up to these from derived classes
	virtual bool open_impl(IndexOpenMode openmode);
	virtual bool close_impl();

private:
	virtual PartitionMetadata get_partition_metadata_impl() = 0;
	virtual region_count_t get_num_regions_impl() = 0;

	virtual uint64_t compute_regions_size_impl(region_id_t lb, region_id_t ub) = 0; // [lb, ub)
	virtual bool read_regions_impl(const std::set< region_id_t > &regions_to_read, std::map< region_id_t, boost::shared_ptr< RegionEncoding > > &regions) = 0;

	virtual void set_partition_metadata_impl(PartitionMetadata metadata) = 0;
	virtual bool write_regions_impl(const std::vector< boost::shared_ptr< RegionEncoding > > &regions) = 0;

protected:
	void assign_partition_id(partition_id_t partition_id) { this->partition_id = partition_id; }

private:
	IndexOpenMode openmode;
	boost::optional< partition_id_t > partition_id;

	friend class IndexIO;
};

#endif /* INDEX_IO_HPP_ */

