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
 * sharedfile-format-index-io.hpp
 *
 *  Created on: Sep 5, 2014
 *      Author: David A. Boyuka II
 */
#ifndef SHAREDFILE_FORMAT_INDEX_IO_HPP_
#define SHAREDFILE_FORMAT_INDEX_IO_HPP_

#include <boost/smart_ptr.hpp>
#include <boost/none.hpp>
#include <boost/optional.hpp>

#include "pique/io/index-io.hpp"
#include "pique/util/datatypes.hpp"

/*
 * An abstract helper base class, which must be derived to fill in the actual file IO details.
 * The assumptions/guarantees built into this base class are as follows:
 * > The index will be stored in a single, contiguous file or similar backend
 * > It must be possible to open an std::istream or std::ostream into this file at any time (although istreams will only be requested in read mode and ostreams in write mode)
 * > Partition IDs will be dense; that is, for a file with N partitions, the partition IDs will be 0 through N-1
 * > Partitions commited to the central IndexIO (see on_partition_committed) shall be packed sequentially in file
 */
class SharedFileFormatIndexIO : public IndexIO {
public:
	SharedFileFormatIndexIO();
	virtual ~SharedFileFormatIndexIO() { this->close(); }

private:
	// Return based on internal metadata. Valid in both read and write mode; override to return error if they shouldn't be valid in write mode.
	virtual GlobalMetadata get_global_metadata_impl();
	virtual std::vector< GlobalPartitionMetadata > get_partition_metadata_impl(partition_id_t begin_partition, partition_id_t end_partition);

protected:
	void init_metadata_empty();
	void read_metadata_from_disk();
	void write_metadata_to_disk(); // Call to write IndexIO metadata to disk if in write mode (usually called in close_impl())

	// Accepts any committed partition_id_t, or one past the end, which will return the location where the next committed partition will be located
	uint64_t get_partition_offset_in_file(partition_id_t partition_id);

	// Stores appropriate metadata for the partition, and returns an assigned partition ID. Usually
	// called by child IndexPartitionIOs as part of its commit_partition().
	partition_id_t commit_partition(uint64_t partition_offset_in_file, uint64_t partition_size, GlobalPartitionMetadata pmeta);

	// Derived implementation to-do:
protected:
	// Always call up to these from derived classes
	virtual bool open_impl(std::string path, IndexOpenMode openmode) = 0;
	virtual bool close_impl() = 0;

private:
	virtual boost::shared_ptr< IndexPartitionIO > get_partition_impl(partition_id_t partition) = 0;
	virtual boost::shared_ptr< IndexPartitionIO > append_partition_impl() = 0;

	// Implemented by derived classes, these functions provide a window into the actual underlying shared file
	// for_reading_large_data indicates whether a large amount of data is expected to be read from/written to
	// the file through this stream. Metadata uses false, region data uses true. May safely be ignored, provided
	// only for the purpose of potential optimization.
	virtual std::istream & open_input_stream(bool for_reading_large_data) = 0;
	virtual std::ostream & open_output_stream(bool for_reading_large_data) = 0;
	virtual void close_input_stream(std::istream &in) = 0;
	virtual void close_output_stream(std::ostream &out) = 0;

private:
	// Header/footer structs for (de)serialization
	struct segment_offsets_header_t {
		uint64_t partition_segment_offset;
		uint64_t footer_segment_offset;

	    friend class boost::serialization::access;
	    template<class Archive> void serialize(Archive & ar, const unsigned int version);
	};
	struct footer_t {
		std::vector< uint64_t > partition_offsets; // Absolute in file, last offset is end offset (i.e. N+1 offsets for N partitions)
		std::vector< GlobalPartitionMetadata > partition_metadatas; // Just use GlobalPartitionMetadata directly, since we aren't doing anything fancy here

		friend class boost::serialization::access;
	    template<class Archive> void serialize(Archive & ar, const unsigned int version);
	};

	SerializableChunk< segment_offsets_header_t > segment_offsets_header;
	SerializableChunk< footer_t > footer;

	friend class SharedFileFormatIndexPartitionIO;
};

class SharedFileFormatIndexPartitionIO : public IndexPartitionIO {
public:
	SharedFileFormatIndexPartitionIO(SharedFileFormatIndexIO &parent, boost::optional< partition_id_t > partition_id, boost::optional< uint64_t > partition_offset_in_file);
	virtual ~SharedFileFormatIndexPartitionIO() { this->close(); }

	virtual IOStats get_io_stats() const { return region_io_stats + partition_header.get_stats(); }
	virtual void reset_io_stats() { region_io_stats = IOStats(); }

private:
	virtual PartitionMetadata get_partition_metadata_impl();
	virtual region_count_t get_num_regions_impl();

	virtual uint64_t compute_regions_size_impl(region_id_t lb, region_id_t ub); // [lb, ub)
	virtual bool read_regions_impl(const std::set< region_id_t > &regions_to_read, std::map< region_id_t, boost::shared_ptr< RegionEncoding > > &regions);

	virtual void set_partition_metadata_impl(PartitionMetadata metadata);
	virtual bool write_regions_impl(const std::vector< boost::shared_ptr< RegionEncoding > > &regions);

protected:
	void init_metadata_empty();
	void read_metadata_from_disk();
	void write_partition_to_disk();

protected:
	// Default implementation of open calls init_metadata_empty()/read_metadata_from_disk as appropriate
	// Default implemenation of close calls write_partition_to_disk() if in write mode
	// Optional: override for non-default behavior
	// Always call up to these from derived classes (if overridden)
	virtual bool open_impl(IndexOpenMode openmode);
	virtual bool close_impl();
private:

	// Used by derived classes to specify whether the size of the partition need be computed
	// in order to allocate it.
	virtual bool is_partition_size_needed_to_allocate_partition() const = 0;

	// Called exactly once at some point prior to the completion of close(). Must return
	// a byte offset in the file at which this partition may safely be written.
	// Note: partition_size == 0 iff !is_partition_size_needed_to_allocate_partition()
	virtual uint64_t allocate_partition(uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta) const = 0;

	// Called by commit_partition once the partition is allocated and written.
	// Usually, this will call IndexIO's commit_partition and assign this
	// partition's ID based on its return, but other behaviors are acceptable
	// (e.g., in some implementations, all the work may be done during allocate_partition())
	virtual void on_partition_committed(uint64_t partition_offset_in_file, uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta) = 0;

private:
	struct partition_header_t {
		PartitionMetadata pmeta; // Just store this directly, since we're not doing anything fancy

		std::vector<uint64_t> region_offsets; // Relative to partition, including skip over header. Last offset is partition end offset (i.e., nbins+1 offsets exist)

	    friend class boost::serialization::access;
	    template<class Archive> void serialize(Archive & ar, const unsigned int version);
	};

	SharedFileFormatIndexIO &parent;
	SerializableChunk< partition_header_t > partition_header;

	boost::optional< uint64_t > partition_offset_in_file; // Always set in read mode, set in write mode after a call to allocate_partition

	std::vector< boost::shared_ptr< RegionEncoding > > regions_to_write; // Buffering from the write_regions function

	mutable IOStats region_io_stats;

	friend class SharedFileFormatIndexIO;
};

#endif /* SHAREDFILE_FORMAT_INDEX_IO_HPP_ */
