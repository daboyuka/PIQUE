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
 * mpi-index-io-control.hpp
 *
 *  Created on: Sep 4, 2014
 *      Author: David A. Boyuka II
 */
#ifndef MPI_INDEX_IO_CONTROL_HPP_
#define MPI_INDEX_IO_CONTROL_HPP_

#include <cstdint>
#include <vector>
#include <boost/smart_ptr.hpp>

#include <mpi.h>

#include "pique/util/openclose.hpp"
#include "pique/io/index-io.hpp"
#include "pique/parallel/io/mpi-index-io.hpp"

#include "pique/stats/stats.hpp"

/*
 * Helps MPIIndexIO/MPIIndexPartitionIO submit coordination requests
 */
class MPIIndexIOWriteControl : public OpenableCloseable<> {
public:
	MPIIndexIOWriteControl(MPIIndexIO &indexio, MPI_Comm comm, int masterrank) :
		indexio(indexio), comm(comm), masterrank(masterrank)
	{
		MPI_Comm_rank(comm, &this->rank);
		MPI_Comm_size(comm, &this->size);
	}
	virtual ~MPIIndexIOWriteControl() { close(); }
	static boost::shared_ptr< MPIIndexIOWriteControl > create_write_control(MPIIndexIO &indexio, MPI_Comm comm, int masterrank);

	virtual bool is_master() const { return false; }

	void update();

	virtual uint64_t allocate_and_commit_partition(uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta);

	MPIStats get_stats() const { return stats; }
	void reset_stats() { stats = MPIStats(); }

private:
	virtual bool open_impl() { return true; }
	virtual bool close_impl();
	virtual void update_impl();

protected:
	static constexpr int MAX_MESSAGE_SIZE = sizeof(uint64_t);
	static constexpr int OBTAIN_PARTITION_SPACE_TAG = 1;
	static constexpr int CLOSING_TAG = 2;

protected:
	MPIIndexIO &indexio;
	const MPI_Comm comm;
	const int masterrank;
	int rank, size;

	mutable MPIStats stats;
};

/*
 * Handles coordination across the MPIIndexIO instances on all processes.
 * User code must periodically call update() to ensure optimal progress.
 * The user may call close to shutdown the master, which will block until
 * all processes are ready to stop.
 */
class MPIIndexIOWriteMasterControl : public MPIIndexIOWriteControl {
public:
	MPIIndexIOWriteMasterControl(MPIIndexIO &indexio, MPI_Comm comm, int masterrank) :
		MPIIndexIOWriteControl(indexio, comm, masterrank), closed_clients(0)
	{}
	virtual ~MPIIndexIOWriteMasterControl() {}

	virtual bool is_master() const { return true; }

	virtual uint64_t allocate_and_commit_partition(uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta);

private:
	virtual void update_impl();
	virtual bool close_impl();

	void process_message(MPI_Status status);

	int closed_clients;
};


#endif /* MPI_INDEX_IO_CONTROL_HPP_ */
