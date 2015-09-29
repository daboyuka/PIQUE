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
 * mpi-index-io-control.cpp
 *
 *  Created on: Sep 4, 2014
 *      Author: David A. Boyuka II
 */

#include "pique/parallel/io/impl/mpi-index-io-control.hpp"

boost::shared_ptr< MPIIndexIOWriteControl > MPIIndexIOWriteControl::create_write_control(MPIIndexIO &indexio, MPI_Comm comm, int masterrank) {
	int rank;
	MPI_Comm_rank(comm, &rank);

	if (rank == masterrank)
		return boost::make_shared< MPIIndexIOWriteMasterControl >(indexio, comm, masterrank);
	else
		return boost::make_shared< MPIIndexIOWriteControl >(indexio, comm, masterrank);
}

// Main message processing center on the master control
void MPIIndexIOWriteMasterControl::process_message(MPI_Status status) {
	const int source = status.MPI_SOURCE;
	const int tag = status.MPI_TAG;

	switch (status.MPI_TAG) {
	case OBTAIN_PARTITION_SPACE_TAG:
	{
		uint64_t msg[3];
		MPI_Recv(msg, 3, MPI_UNSIGNED_LONG_LONG, source, tag, this->comm, MPI_STATUS_IGNORE);
		const uint64_t partition_size = msg[0];
		const IndexIO::GlobalPartitionMetadata pmeta { { msg[1], msg[2] } };

		uint64_t return_offset = this->allocate_and_commit_partition(partition_size, pmeta); // call the master-local version
		MPI_Send(&return_offset, 1, MPI_UNSIGNED_LONG_LONG, source, tag, this->comm);

		break;
	}
	case CLOSING_TAG:
		MPI_Recv(nullptr, 0, MPI_BYTE, source, tag, this->comm, MPI_STATUS_IGNORE); // Pop the message from the source/tag so it isn't visited again
		++this->closed_clients;
		break;
	}
}

// update()
void MPIIndexIOWriteControl::update() { this->update_impl(); }

void MPIIndexIOWriteControl::update_impl() {}

void MPIIndexIOWriteMasterControl::update_impl() {
	int is_message_waiting;
	MPI_Status status;

	TIME_STATS_TIME_BEGIN(stats.mpitime)
	while (
		MPI_Iprobe(
			MPI_ANY_SOURCE, MPI_ANY_TAG,
			this->comm,
			&is_message_waiting, &status) == MPI_SUCCESS
		&& is_message_waiting)
	{
		process_message(status);
	}
	TIME_STATS_TIME_END()
}

// close()
bool MPIIndexIOWriteControl::close_impl() {
	TIME_STATS_TIME_BEGIN(stats.mpitime)
	MPI_Send(nullptr, 0, MPI_BYTE, this->masterrank, CLOSING_TAG, this->comm);
	TIME_STATS_TIME_END()

	return true;
}

bool MPIIndexIOWriteMasterControl::close_impl() {
	++this->closed_clients;

	TIME_STATS_TIME_BEGIN(stats.mpitime)
	// Process messages and wait until all clients have closed, as well
	MPI_Status status;
	while (this->closed_clients < this->size) {
		MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, this->comm, &status);
		process_message(status);
	}
	TIME_STATS_TIME_END()

	return true;
}

// obtain_partition_space()
uint64_t MPIIndexIOWriteControl::allocate_and_commit_partition(uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta) {
	TIME_STATS_TIME_BEGIN(stats.mpitime)
	uint64_t msg[3] = { partition_size, gpmeta.domain.first, gpmeta.domain.second };
	MPI_Send(msg, 3, MPI_UNSIGNED_LONG_LONG, this->masterrank, OBTAIN_PARTITION_SPACE_TAG, this->comm);

	uint64_t offset;
	MPI_Recv(&offset, 1, MPI_UNSIGNED_LONG_LONG, this->masterrank, OBTAIN_PARTITION_SPACE_TAG, this->comm, MPI_STATUS_IGNORE);

	return offset;
	TIME_STATS_TIME_END()
}

uint64_t MPIIndexIOWriteMasterControl::allocate_and_commit_partition(uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta) {
	using partition_id_t = IndexIOTypes::partition_id_t;

	const partition_id_t partid = this->indexio.get_num_partitions();           // Determine the next unallocated partition ID
	const uint64_t offset = this->indexio.get_partition_offset_in_file(partid); // Determine the next unallocated partition's offset in file (i.e., the end offset of all current partitions)
	this->indexio.commit_partition(offset, partition_size, gpmeta);             // Commit this new partition using this information

	this->update(); // Take this opportunity to process any pending messages

	return offset;
}


