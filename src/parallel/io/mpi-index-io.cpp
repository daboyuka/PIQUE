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
 * mpi-index-io.cpp
 *
 *  Created on: Sep 3, 2014
 *      Author: David A. Boyuka II
 */

#include <boost/array.hpp>
#include <boost/none.hpp>

#include "pique/parallel/util/mpi-stream.hpp"
#include "pique/parallel/io/mpi-index-io.hpp"
#include "pique/parallel/io/impl/mpi-index-io-control.hpp"


MPIIndexIO::MPIIndexIO(MPI_Comm comm, int masterrank) :
	SharedFileFormatIndexIO(), file(MPI_FILE_NULL)
{
	MPI_Comm_dup(comm, &this->comm);
	this->control = MPIIndexIOWriteControl::create_write_control(*this, this->comm, masterrank);
}

MPIIndexIO::~MPIIndexIO() {
	this->close();
	MPI_Comm_free(&this->comm);
}

bool MPIIndexIO::open_impl(std::string path, IndexOpenMode openmode) {
	assert(openmode == IndexOpenMode::READ || openmode == IndexOpenMode::WRITE);
	if (!this->SharedFileFormatIndexIO::open_impl(path, openmode))
		return false;

	this->control->open();

	if (openmode == IndexOpenMode::WRITE) {
		MPI_File_open(comm, (char*)path.c_str() /* coerce to non-const because sucky API */, MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &this->file);
		this->ofstream.open(this->file, this->write_buffer_size);

		if (this->control->is_master()) {
			this->init_metadata_empty(); // Call on base class
		} else {
			; // Do nothing
		}
	} else /* READ mode */ {
		abort();
	}

	return true;
}

bool MPIIndexIO::close_impl() {
	const IndexOpenMode openmode = this->get_open_mode();
	if (!this->SharedFileFormatIndexIO::close_impl()) {
		MPI_Abort(this->comm, 1);
		return false;
	}

	const bool is_master = this->control->is_master();
	this->control->close();

	if (is_master && openmode == IndexOpenMode::WRITE)
		master_finalize_file();

	MPI_File_close(&this->file);

	// Use a barrier to ensure no process finishes close() until the file is finalized,
	// to avoid errors in attempting to open the same file immediately afterward
	MPI_Barrier(this->comm);
	return true;
}

void MPIIndexIO::master_finalize_file() {
	this->write_metadata_to_disk();
}



boost::shared_ptr< IndexPartitionIO > MPIIndexIO::get_partition_impl(partition_id_t partition) {
	assert(this->get_open_mode() == IndexOpenMode::READ);
	abort();
	return nullptr;
}

boost::shared_ptr< IndexPartitionIO > MPIIndexIO::append_partition_impl() {
	assert(this->get_open_mode() == IndexOpenMode::WRITE);
	return boost::make_shared< MPIIndexPartitionIO >(*this, boost::none, boost::none);
}

void MPIIndexIO::mpi_update() {
	this->control->update();
}

MPIIndexIOStats MPIIndexIO::get_stats() const {
	MPIIndexIOStats stats;
	stats.mpistats = control->get_stats();
	//stats.num_read_reqs = ifstream.get_mpi_device().get_read_request_count();
	stats.num_write_reqs = ofstream.get_mpi_device().get_write_request_count();
	stats.num_seeks = ofstream.get_mpi_device().get_seek_count();
	return stats;
}

void MPIIndexIO::reset_stats() {
	return control->reset_stats();
}

uint64_t MPIIndexPartitionIO::allocate_partition(uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta) const {
	return get_control().allocate_and_commit_partition(partition_size, gpmeta);
}

void MPIIndexPartitionIO::on_partition_committed(uint64_t partition_offset_in_file, uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta) {
	// Do nothing; the partition was already committed allocate_partition
}
