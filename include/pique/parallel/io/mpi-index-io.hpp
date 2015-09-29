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
 * mpi-index-io.hpp
 *
 *  Created on: Sep 3, 2014
 *      Author: David A. Boyuka II
 */
#ifndef MPI_INDEX_IO_HPP_
#define MPI_INDEX_IO_HPP_

#include <cstdint>
#include <boost/optional.hpp>

#include <mpi.h>

#include "pique/util/datatypes.hpp"
#include "pique/io/index-io.hpp"
#include "pique/io/formats/sharedfile/sharedfile-format-index-io.hpp"
#include "pique/parallel/util/mpi-stream.hpp"

#include "pique/stats/stats.hpp"

class MPIIndexIOWriteControl; // Forward declaration for incomplete type, used in a pointer in MPIIndexIO, defined in mpi-index-io-control.hpp

struct MPIStats : public BaseStats< MPIStats > {
	template<typename CombineFn> void combine(const MPIStats &other, CombineFn combine) {
		combine(this->mpitime, other.mpitime);
	}

	TimeStats mpitime;
};

DEFINE_STATS_SERIALIZE(MPIStats, stats, stats.mpitime);

struct MPIIndexIOStats : public BaseStats< MPIIndexIOStats > {
	template<typename CombineFn> void combine(const MPIIndexIOStats &other, CombineFn combine) {
		combine(this->mpistats, other.mpistats);
		combine(this->num_seeks, other.num_seeks);
		combine(this->num_read_reqs, other.num_read_reqs);
		combine(this->num_write_reqs, other.num_write_reqs);
	}

	MPIStats mpistats;
	uint64_t num_seeks{0};
	uint64_t num_read_reqs{0};
	uint64_t num_write_reqs{0};
};

DEFINE_STATS_SERIALIZE(MPIIndexIOStats, stats, stats.mpistats & stats.num_seeks & stats.num_read_reqs & stats.num_write_reqs);

class MPIIndexIO : public SharedFileFormatIndexIO {
public:
	MPIIndexIO(MPI_Comm comm = MPI_COMM_WORLD, int masterrank = 0);
	virtual ~MPIIndexIO();

	// May be called periodically to optimize MPI communications
	void mpi_update();

	MPIIndexIOStats get_stats() const;
	void reset_stats();

private:
	virtual bool open_impl(std::string path, IndexOpenMode openmode);
	virtual bool close_impl();

	virtual boost::shared_ptr< IndexPartitionIO > get_partition_impl(partition_id_t partition);
	virtual boost::shared_ptr< IndexPartitionIO > append_partition_impl();

	// Implemented by derived classes, these functions provide a window into the actual underlying shared file
	// for_reading_large_data indicates whether a large amount of data is expected to be read from/written to
	// the file through this stream. Metadata uses false, region data uses true. May safely be ignored, provided
	// only for the purpose of potential optimization.
	virtual std::istream & open_input_stream(bool for_reading_large_data) { return ifstream; }
	virtual std::ostream & open_output_stream(bool for_reading_large_data) { return ofstream; }
	virtual void close_input_stream(std::istream &in) {}
	virtual void close_output_stream(std::ostream &out) {}

private:
	void master_finalize_file();

private:
	// Constants (for now)
	const uint64_t read_buffer_size = (1ULL << 20);
	const uint64_t write_buffer_size = (1ULL << 27);

	// MPI communicator information
	MPI_Comm comm;

	// MPI file information
	MPI_File file;
	MPIFileBufferedStream ifstream;
	MPIFileBufferedStream ofstream;

	// MPI control
	boost::shared_ptr< MPIIndexIOWriteControl > control;

	friend class MPIIndexPartitionIO;
	friend class MPIIndexIOWriteControl;
	friend class MPIIndexIOWriteMasterControl;
};



class MPIIndexPartitionIO : public SharedFileFormatIndexPartitionIO {
public:
	using bin_id_t				= BinnedIndexTypes::bin_id_t;
	using bin_count_t			= BinnedIndexTypes::bin_count_t;
	using region_id_t			= BinnedIndexTypes::region_id_t;
	using region_count_t		= BinnedIndexTypes::region_count_t;

public:
	MPIIndexPartitionIO(MPIIndexIO &indexio, boost::optional< partition_id_t > partition_id, boost::optional< uint64_t > partition_offset_in_file) :
		SharedFileFormatIndexPartitionIO(indexio, partition_id, partition_offset_in_file), indexio(indexio)
	{}
	virtual ~MPIIndexPartitionIO() { this->close(); }

private:
	virtual bool is_partition_size_needed_to_allocate_partition() const { return true; }

	// Called exactly once at some point prior to the completion of close(). Must return
	// a byte offset at which this partition may safely be written.
	// Note: partition_size == 0 iff !is_partition_size_needed_to_allocate_partition()
	virtual uint64_t allocate_partition(uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta) const;

	// Called by commit_partition once the partition is allocated and written.
	// Usually, this will call IndexIO's commit_partition and assign this
	// partition's ID based on its return, but other behaviors are acceptable
	// (e.g., in some implementations, all the work may be done during allocate_partition())
	virtual void on_partition_committed(uint64_t partition_offset_in_file, uint64_t partition_size, IndexIO::GlobalPartitionMetadata gpmeta);

private:
	MPIIndexIOWriteControl & get_control() const { return *indexio.control; }

private:
	MPIIndexIO &indexio;

	friend class MPIIndexIO;
};

#endif /* MPI_INDEX_IO_HPP_ */
