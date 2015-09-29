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
 * posix-index-io.hpp
 *
 *  Created on: May 7, 2014
 *      Author: David A. Boyuka II
 */
#ifndef POSIX_INDEX_IO_HPP_
#define POSIX_INDEX_IO_HPP_

#include <boost/smart_ptr.hpp>
#include <boost/none.hpp>
#include <boost/optional.hpp>
#include <boost/serialization/shared_ptr.hpp>

#include "pique/io/index-io.hpp"
#include "pique/io/formats/sharedfile/sharedfile-format-index-io.hpp"
#include "pique/util/datatypes.hpp"

class POSIXIndexIO : public SharedFileFormatIndexIO {
public:
	virtual ~POSIXIndexIO() { this->close(); }

private:
	virtual bool open_impl(std::string path, IndexOpenMode openmode);
	virtual bool close_impl();

	virtual boost::shared_ptr< IndexPartitionIO > get_partition_impl(partition_id_t partition);
	virtual boost::shared_ptr< IndexPartitionIO > append_partition_impl();

	virtual std::istream & open_input_stream(bool for_reading_large_data) { return index_file; }
	virtual std::ostream & open_output_stream(bool for_reading_large_data) { return index_file; }
	virtual void close_input_stream(std::istream &in) {}
	virtual void close_output_stream(std::ostream &out) {}

private:
	std::fstream index_file;

	friend class POSIXIndexPartitionIO;
};

class POSIXIndexPartitionIO : public SharedFileFormatIndexPartitionIO {
public:
	// Implementation-specific: partition ID is always known, so always pass it up
	POSIXIndexPartitionIO(POSIXIndexIO &parent, partition_id_t partition_id, boost::optional< uint64_t > partition_offset_in_file);
	virtual ~POSIXIndexPartitionIO() { this->close(); }

private:
	virtual bool is_partition_size_needed_to_allocate_partition() const { return false; }

	virtual uint64_t allocate_partition(uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta) const;
	virtual void on_partition_committed(uint64_t partition_offset_in_file, uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta);

private:
	POSIXIndexIO &parent;

	friend class POSIXIndexIO;
};

#endif /* POSIX_INDEX_IO_HPP_ */
