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
 * sharedfile-format-index-io.cpp
 *
 *  Created on: Sep 5, 2014
 *      Author: David A. Boyuka II
 */

#include <vector>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/serialization/vector.hpp>

// To (de)serialize IndexEncoding pointers
#include "pique/encoding/index-encoding.hpp"
#include "pique/encoding/index-encoding-serialization.hpp"

#include "pique/io/formats/sharedfile/sharedfile-format-index-io.hpp"
#include "pique/util/timing.hpp"

BOOST_CLASS_IMPLEMENTATION(SharedFileFormatIndexIO::segment_offsets_header_t, boost::serialization::object_class_info) // yes version information serialized
template<class Archive> void SharedFileFormatIndexIO::segment_offsets_header_t::serialize(Archive & ar, const unsigned int version) {
	ar & this->partition_segment_offset;
	ar & this->footer_segment_offset;
}

// Serialize GlobalPartitionMetadata
namespace boost { namespace serialization {
template<class Archive>
void serialize(Archive & ar, IndexIO::GlobalPartitionMetadata &gpmeta, const unsigned int version) {
    ar & gpmeta.domain.first;
    ar & gpmeta.domain.second;
}
}} // namespace

BOOST_CLASS_IMPLEMENTATION(SharedFileFormatIndexIO::footer_t, boost::serialization::object_serializable) // no version information serialized
template<class Archive> void SharedFileFormatIndexIO::footer_t::serialize(Archive & ar, const unsigned int version) {
	ar & this->partition_offsets;
	ar & this->partition_metadatas;
}

// Serialize PartitionMetadata
namespace boost { namespace serialization {
template<class Archive>
void serialize(Archive & ar, IndexPartitionIO::PartitionMetadata &pmeta, const unsigned int version) {
	using DTID = Datatypes::IndexableDatatypeID;
	using BinningType = AbstractBinningSpecification::BinningSpecificationType;
	if (Archive::is_loading::value)
		pmeta.init_empty();

	ar & reinterpret_cast<char&>(*pmeta.indexed_datatype);
	ar & pmeta.domain->first;
	ar & pmeta.domain->second;
	serialize_encoding(ar, pmeta.index_enc);
	ar & reinterpret_cast<char&>(*pmeta.index_rep);
	serialize_binning_type(ar, pmeta.binning_spec);
}
}} // namespace

BOOST_CLASS_IMPLEMENTATION(SharedFileFormatIndexPartitionIO::partition_header_t, boost::serialization::object_serializable) // no version information serialized
template<class Archive> void SharedFileFormatIndexPartitionIO::partition_header_t::serialize(Archive & ar, const unsigned int version) {
	ar & this->pmeta;
	ar & this->region_offsets;
}

SharedFileFormatIndexIO::SharedFileFormatIndexIO() :
	segment_offsets_header(
		[    ](){return 0;},
		[    ](){return std::ios::beg;}),
	footer(
		[this](){return this->segment_offsets_header->footer_segment_offset;},
		[    ](){return std::ios::beg;})
{}

bool SharedFileFormatIndexIO::open_impl(std::string path, IndexOpenMode openmode) {
	return this->IndexIO::open_impl(path, openmode);
}

bool SharedFileFormatIndexIO::close_impl() {
	return this->IndexIO::close_impl();
}

auto SharedFileFormatIndexIO::get_global_metadata_impl() -> GlobalMetadata {
	GlobalMetadata gmeta;
	// Compute the partition ID array based on the assumption of dense/packed partition IDs
	gmeta.partitions = std::vector< partition_id_t >(
		boost::counting_iterator< partition_id_t >(0),
		boost::counting_iterator< partition_id_t >(this->footer->partition_metadatas.size())
	);
	return gmeta;
}

auto SharedFileFormatIndexIO::get_partition_metadata_impl(partition_id_t begin_partition, partition_id_t end_partition) -> std::vector< GlobalPartitionMetadata > {
	std::vector< GlobalPartitionMetadata > gpmetas(
		this->footer->partition_metadatas.begin() + begin_partition,
		this->footer->partition_metadatas.begin() + end_partition
	);
	return gpmetas;
}

void SharedFileFormatIndexIO::init_metadata_empty() {
	// First, initialize the segment offsets header, computing its eventual size on disk, then initializing the other offsets
	this->segment_offsets_header = segment_offsets_header_t();

	const uint64_t soh_size = this->segment_offsets_header.measure();
	this->segment_offsets_header->partition_segment_offset = soh_size;
	this->segment_offsets_header->footer_segment_offset = soh_size;

	this->footer = footer_t();
	this->footer->partition_offsets.push_back(soh_size); // The last partition offset is the offset for the next partition to be inserted
}

void SharedFileFormatIndexIO::read_metadata_from_disk() {
	std::istream &fin = open_input_stream(false);
	this->segment_offsets_header.read(fin);
	this->footer.read(fin);
	close_input_stream(fin);
}

void SharedFileFormatIndexIO::write_metadata_to_disk() {
	// Update the footer offset (i.e., the end of the partition segment)
	this->segment_offsets_header->footer_segment_offset =
		this->footer->partition_offsets.back();

	std::ostream &fout = open_output_stream(false);
	this->segment_offsets_header.write(fout);
	this->footer.write(fout);
	close_output_stream(fout);
}

uint64_t SharedFileFormatIndexIO::get_partition_offset_in_file(partition_id_t partition_id) {
	assert(partition_id <= get_num_partitions()); // < means already committed, == means next-to-be-committed; both are fine
	return this->footer->partition_offsets.at(partition_id);
}

auto SharedFileFormatIndexIO::commit_partition(uint64_t partition_offset_in_file, uint64_t partition_size, GlobalPartitionMetadata pmeta) -> partition_id_t {
	const partition_id_t next_partid = get_num_partitions();

	assert(partition_offset_in_file == this->footer->partition_offsets[next_partid]); // Ensure we are writing this partition immediately after the end of the previous one
	this->footer->partition_offsets.push_back(partition_offset_in_file + partition_size);
	this->footer->partition_metadatas.push_back(pmeta);
	return next_partid;
}

SharedFileFormatIndexPartitionIO::SharedFileFormatIndexPartitionIO(SharedFileFormatIndexIO& parent, boost::optional< partition_id_t > partition_id, boost::optional< uint64_t > partition_offset_in_file) :
	IndexPartitionIO(partition_id),
	parent(parent),
	partition_header(
		[this](){ return *this->partition_offset_in_file; },
		[    ](){ return std::ios::beg; }
	),
	partition_offset_in_file(partition_offset_in_file)
{}

bool SharedFileFormatIndexPartitionIO::open_impl(IndexOpenMode openmode) {
	if (!this->IndexPartitionIO::open_impl(openmode))
		return false;

	if (openmode == IndexOpenMode::WRITE)
		init_metadata_empty();
	else if (openmode == IndexOpenMode::READ)
		read_metadata_from_disk();
	else
		abort();
	return true;
}

bool SharedFileFormatIndexPartitionIO::close_impl() {
	const IndexOpenMode openmode = this->get_open_mode();
	if (!this->IndexPartitionIO::close_impl())
		return false;

	if (openmode == IndexOpenMode::WRITE)
		write_partition_to_disk();

	this->partition_header = boost::none;
	this->partition_offset_in_file = boost::none;
	this->regions_to_write.clear();

	return true;
}

void SharedFileFormatIndexPartitionIO::init_metadata_empty() {
	this->partition_header = partition_header_t();

	// Since we are in write mode, clear the partition offset (just in case)
	this->partition_offset_in_file = boost::none;
}

void SharedFileFormatIndexPartitionIO::read_metadata_from_disk() {
	std::istream &fin = this->parent.open_input_stream(false);
	this->partition_header.read(fin);
	this->parent.close_input_stream(fin);

	// Since we are in read mode, go ahead and determine the partition offset now
	this->partition_offset_in_file = this->parent.footer->partition_offsets[this->get_partition_id()];
}

void SharedFileFormatIndexPartitionIO::write_partition_to_disk() {
	assert(this->partition_header && this->partition_header->pmeta.is_filled());

	IndexIO::GlobalPartitionMetadata gpmeta;
	gpmeta.domain = *this->partition_header->pmeta.domain;

	// Make placeholders for the region offsets to get an accurate size for the partition header
	this->partition_header->region_offsets.clear();
	this->partition_header->region_offsets.resize(regions_to_write.size() + 1, 0); // Placeholders for the purpose of accurate computing of the header size
	const uint64_t header_size = this->partition_header.measure();

	// BUGFIX: some ostream implementations are broken, with tellp flushing the buffer
	// Therefore, we now always precompute the region sizes to build the offset vector first

	// Compute region offsets and the total partition size
	this->partition_header->region_offsets[0] = header_size;
	measuring_stream mstream;
	for (region_id_t region_id = 0; region_id < regions_to_write.size(); ++region_id) {
		boost::shared_ptr< RegionEncoding > region = regions_to_write[region_id];
		region->save_to_stream(mstream);
		this->partition_header->region_offsets[region_id + 1] = header_size + mstream.get_byte_count();
	}
	const uint64_t partition_size = header_size + mstream.get_byte_count();

	// Allocate space for this partition
	const uint64_t partition_offset_in_file = this->allocate_partition(partition_size, gpmeta);
	this->partition_offset_in_file = partition_offset_in_file;

	// Open the output stream
	std::ostream &fout = this->parent.open_output_stream(true);

	// First, write the partition header, which will implicitly seek to the beginning of the partition before writing
	this->partition_header.write(fout);

	// Then, write all the regions to disk
	// (the write position will be just after the partition header upon entering this block)
	{ // IO timing
		TimeBlock iotime = this->region_io_stats.open_write_time_block();

		// Write all of the regions, recording the end offset of each one
		// (also assert that they are the right representation type)
		for (boost::shared_ptr< RegionEncoding > region : regions_to_write) {
			assert(region->get_type() == *this->partition_header->pmeta.index_rep);
			region->save_to_stream(fout);
		}

		fout.flush();
	}

	// Compute the actual number of bytes written, and ensure it is the expected amount
	const uint64_t final_partition_size = (uint64_t)fout.tellp() - partition_offset_in_file; // tellp() is OK here even if it's broken, since we just flushed
	assert(partition_size == final_partition_size);

	this->region_io_stats.write_bytes += final_partition_size - header_size;

	// Close the output stream
	this->parent.close_output_stream(fout);

	// Commit the partition
	this->on_partition_committed(partition_offset_in_file, final_partition_size, gpmeta);
}

auto SharedFileFormatIndexPartitionIO::get_partition_metadata_impl() -> PartitionMetadata {
	return this->partition_header->pmeta;
}

auto SharedFileFormatIndexPartitionIO::get_num_regions_impl() -> region_count_t {
	return this->partition_header->region_offsets.size() - 1;
}

uint64_t SharedFileFormatIndexPartitionIO::compute_regions_size_impl(region_id_t lb, region_id_t ub) {
	return this->partition_header->region_offsets[ub] - this->partition_header->region_offsets[lb];
}

bool SharedFileFormatIndexPartitionIO::read_regions_impl(const std::set<region_id_t>& regions_to_read, std::map<region_id_t, boost::shared_ptr<RegionEncoding> >& regions) {
	std::vector<char> buffer;

	std::istream &fin = this->parent.open_input_stream(true);

	const uint64_t partition_offset = *this->partition_offset_in_file;

	for (auto region_id_it = regions_to_read.cbegin(); region_id_it != regions_to_read.cend();) {
		const region_id_t run_start = *region_id_it++;
		region_id_t run_end = run_start;
		while (region_id_it != regions_to_read.cend() && *region_id_it == run_end + 1)
			run_end = *region_id_it++;

		// Read regionss in range [run_start, run_end]
		const uint64_t run_start_offset = this->partition_header->region_offsets[run_start];
		const uint64_t run_end_offset = this->partition_header->region_offsets[run_end + 1];
		const uint64_t run_length = run_end_offset - run_start_offset;

		buffer.resize(run_length);

		{
			TimeBlock iotime = this->region_io_stats.open_read_time_block();
			fin.seekg(partition_offset + run_start_offset, std::ios::beg);
			fin.read(&buffer.front(), run_length); // We were careful to resize to the correct length above before doing this...
		}

		this->region_io_stats.read_bytes += run_length;
		this->region_io_stats.read_seeks++;

		boost::iostreams::basic_array_source<char> buffer_array_device(&buffer.front(), buffer.size());
		boost::iostreams::stream< boost::iostreams::basic_array_source<char> > buffer_stream(buffer_array_device);

		for (region_id_t region_id = run_start; region_id <= run_end; region_id++) {
			const uint64_t region_offset_in_partition		= this->partition_header->region_offsets[region_id];
			const uint64_t region_end_offset_in_partition	= this->partition_header->region_offsets[region_id + 1];
			const uint64_t region_offset_in_buffer			= region_offset_in_partition - run_start_offset;
			const uint64_t region_length					= region_end_offset_in_partition - region_offset_in_partition;

			boost::shared_ptr< RegionEncoding > region;

			if (region_length > 0) {
				region = RegionEncoding::make_null_region(*this->partition_header->pmeta.index_rep);

				// Deserialize the region from the correct slice of the byte buffer just read from disk
				buffer_stream.seekg(region_offset_in_buffer, buffer_stream.beg);
				region->load_from_stream(buffer_stream);
			} else {
				region = nullptr;
			}

			regions[region_id] = region;
		}
	}

	this->parent.close_input_stream(fin);
	return true;
}

void SharedFileFormatIndexPartitionIO::set_partition_metadata_impl(PartitionMetadata pmeta) {
	this->partition_header->pmeta.update(pmeta); // Only load in fields that are initialized in pmeta
}

bool SharedFileFormatIndexPartitionIO::write_regions_impl(const std::vector< boost::shared_ptr< RegionEncoding > > &regions) {
	this->regions_to_write = regions; // Hold the regions until close() time
	return true;
}
