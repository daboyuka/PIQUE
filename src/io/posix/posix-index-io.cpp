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
 * posix-index-io.cpp
 *
 *  Created on: May 7, 2014
 *      Author: David A. Boyuka II
 */

#include <iostream>

#include <cstdint>
#include <vector>
#include <set>
#include <map>

#include <boost/smart_ptr.hpp>
#include <boost/none.hpp>

#include "pique/indexing/binning-spec.hpp"
#include "pique/io/index-io.hpp"
#include "pique/io/posix/posix-index-io.hpp"
#include "pique/util/timing.hpp"

bool POSIXIndexIO::open_impl(std::string path, IndexOpenMode openmode) {
	if (!this->SharedFileFormatIndexIO::open_impl(path, openmode))
		return false;

	if (this->index_file.is_open())
		return false;

	const std::ios::openmode file_openmode =
			std::ios::binary |
			(openmode == IndexOpenMode::READ ?
				std::ios::in :
				std::ios::out);

	this->index_file.open(path.c_str(), file_openmode);
	if (this->index_file.fail())
		return false;

	if (openmode == IndexOpenMode::READ)
		this->read_metadata_from_disk(); // Call on base class
	else if (openmode == IndexOpenMode::WRITE)
		this->init_metadata_empty();
	else
		abort();
	return true;
}

bool POSIXIndexIO::close_impl() {
	const IndexOpenMode openmode = this->get_open_mode();
	if (!this->SharedFileFormatIndexIO::close_impl())
		return false;

	if (openmode == IndexOpenMode::WRITE)
		this->write_metadata_to_disk(); // Call on the base class to write metadata to disk

	this->index_file.close();
	return true;
}

boost::shared_ptr< IndexPartitionIO > POSIXIndexIO::get_partition_impl(partition_id_t partition) {
	return boost::make_shared< POSIXIndexPartitionIO >(*this, partition, this->get_partition_offset_in_file(partition));
}

boost::shared_ptr< IndexPartitionIO > POSIXIndexIO::append_partition_impl() {
	return boost::make_shared< POSIXIndexPartitionIO >(*this, get_num_partitions(), boost::none);
}

POSIXIndexPartitionIO::POSIXIndexPartitionIO(POSIXIndexIO &parent, partition_id_t partition_id, boost::optional< uint64_t > partition_offset_in_file) :
		SharedFileFormatIndexPartitionIO(parent, partition_id, partition_offset_in_file),
		parent(parent)
{}

uint64_t POSIXIndexPartitionIO::allocate_partition(uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta) const {
	assert(this->get_partition_id() == this->parent.get_num_partitions()); // Ensure we are the next partition that is supposed to be written
	return this->parent.get_partition_offset_in_file(this->get_partition_id());
}

void POSIXIndexPartitionIO::on_partition_committed(uint64_t partition_offset_in_file, uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta) {
	assert(this->get_partition_id() == this->parent.get_num_partitions()); // Ensure we are the next partition that is supposed to be written
	this->parent.commit_partition(partition_offset_in_file, partition_size, gpmeta);
}

