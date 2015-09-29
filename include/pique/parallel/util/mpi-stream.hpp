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
 * mpi-stream.hpp
 *
 *  Created on: Sep 3, 2014
 *      Author: David A. Boyuka II
 */
#ifndef MPI_STREAM_HPP_
#define MPI_STREAM_HPP_

#include <cstdint>
#include <iostream>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/positioning.hpp>

#include <mpi.h>

#include "pique/stats/stats.hpp"

class MPIFileStreamDevice : public boost::iostreams::device< boost::iostreams::seekable > {
public:
	using stream_offset = boost::iostreams::stream_offset;

	MPIFileStreamDevice(MPI_File mpifile) :
		mpifile(mpifile), seeks(0), readreqs(0), writereqs(0), bytesread(0), byteswritten(0)
	{}

	template<bool dowrite>
	std::streamsize readwrite(char* s, std::streamsize n) {
		std::streamsize count = 0;

		while (n > 0) {
			int io_chunk_size = std::numeric_limits< int >::max();
			if (io_chunk_size > n)
				io_chunk_size = n;

			MPI_Status status;
			int error = dowrite ?
					MPI_File_write(this->mpifile, (void*)s, io_chunk_size, MPI_BYTE, &status) :
					MPI_File_read(this->mpifile, (void*)s, io_chunk_size, MPI_BYTE, &status);
			if (error != MPI_SUCCESS)
				break;

			int actual_io;
			MPI_Get_count(&status, MPI_BYTE, &actual_io);

			n -= actual_io;
			count += actual_io;
			s += actual_io;

			if (dowrite)
				++this->writereqs;
			else
				++this->readreqs;

			if (actual_io < io_chunk_size) // EOF
				break;
		}

		if (dowrite)
			this->byteswritten += count;
		else
			this->bytesread += count;
		return count;
	}
	std::streamsize read(char* s, std::streamsize n) { return readwrite<false>(s, n); }
	std::streamsize write(const char* s, std::streamsize n) { return readwrite<true>((char*)s, n); }
	stream_offset seek(stream_offset off, std::ios_base::seekdir way) {
		// Advances the read/write head by off characters,
		// returning the new position, where the offset is
		// calculated from:
		//  - the start of the sequence if way == ios_base::beg
		//  - the current position if way == ios_base::cur
		//  - the end of the sequence if way == ios_base::end

		switch (way) {
		case std::ios::beg:
			MPI_File_seek(mpifile, (MPI_Offset)(off), MPI_SEEK_SET);
			break;
		case std::ios::end:
			MPI_File_seek(mpifile, (MPI_Offset)(off), MPI_SEEK_END);
			break;
		case std::ios::cur:
			MPI_File_seek(mpifile, (MPI_Offset)(off), MPI_SEEK_CUR);
			break;
		default:
			abort(); break; // Should never happen
		}

		MPI_Offset pos;
		MPI_File_get_position(mpifile, &pos);

		++seeks;
		return pos;
	}

	// Statistics for debugging and information
	uint64_t get_seek_count() const { return this->seeks; }
	uint64_t get_read_request_count() const { return this->readreqs; }
	uint64_t get_write_request_count() const { return this->writereqs; }
	uint64_t get_bytes_read() const { return this->bytesread; }
	uint64_t get_bytes_written() const { return this->byteswritten; }

private:
	const MPI_File mpifile;

	uint64_t seeks;
	uint64_t readreqs;
	uint64_t writereqs;
	uint64_t bytesread;
	uint64_t byteswritten;
};

class MPIFileBufferedStream : public boost::iostreams::stream< MPIFileStreamDevice > {
public:
	typedef boost::iostreams::stream< MPIFileStreamDevice > ParentType;
	MPIFileBufferedStream() : ParentType() {}
	MPIFileBufferedStream(MPI_File mpifile) : ParentType(MPIFileStreamDevice(mpifile)) {}
	MPIFileBufferedStream(MPI_File mpifile, std::streamsize buffer_size) : ParentType(MPIFileStreamDevice(mpifile), buffer_size) {}

	void open(MPI_File mpifile) { ParentType::open(MPIFileStreamDevice(mpifile)); }
	void open(MPI_File mpifile, std::streamsize buffer_size) { ParentType::open(MPIFileStreamDevice(mpifile), buffer_size); }

	const MPIFileStreamDevice & get_mpi_device() const { return *const_cast<MPIFileBufferedStream&>(*this).component(); }
};

class MPIGatherStreamDevice : public boost::iostreams::device< boost::iostreams::bidirectional > {
public:
	using stream_offset = boost::iostreams::stream_offset;

	struct Stats : public IOStats, public BaseStats< Stats > {
		virtual ~Stats() {}

		template<typename CombineFn> void combine(const Stats &other, CombineFn combine) {
			this->template IOStats::combine< CombineFn >(*this, combine);
			combine(this->read_reqs,  other.read_reqs );
			combine(this->write_reqs, other.write_reqs);
		}

		uint64_t read_reqs{0};
		uint64_t write_reqs{0};
	};

	MPIGatherStreamDevice(MPI_Comm comm, int tag, int root, int nsenders) :
		comm(comm), tag(tag), root(root), nsenders(nsenders), nreceived(0), buffer(), stats()
	{
		this->buffer_start = this->buffer.begin();
		this->buffer_end = this->buffer.end();
	}

	MPIGatherStreamDevice(MPI_Comm comm, int tag, int root) :
		comm(comm), tag(tag), root(root), nreceived(0), buffer(), stats()
	{
		MPI_Comm_size(comm, &this->nsenders);
		--this->nsenders; // Don't count the root
		this->buffer_start = this->buffer.begin();
		this->buffer_end = this->buffer.end();
	}

	std::streamsize read(char* s, std::streamsize n) {
		IO_STATS_READ_TIME_BEGIN(this->stats)

		// If there is no buffered data, do a MPI receive to get more data
		if (this->buffer_start == this->buffer_end) {
			// First check whether we have received from all senders. If so, return EOF
			if (this->nreceived == this->nsenders)
				return 0;

			// Wait for a message to arrive
			MPI_Status status;
			MPI_Probe(MPI_ANY_SOURCE, this->tag, this->comm, &status);
			++this->nreceived; // Record the receipt of another sender's data

			// Determine that message's size
			int recvsize;
			MPI_Get_count(&status, MPI_BYTE, &recvsize);
			if (recvsize == MPI_UNDEFINED)
				return 0;

			// Update stats
			++this->stats.read_reqs;
			this->stats.read_bytes += recvsize;

			// If the user requested at least this many bytes, receive it directly into the user's buffer
			// Else, receive it into the internal buffer and fall through to the buffered read case
			if ((std::streamsize)recvsize <= n) {
				// Receive directly into the user's buffer and return
				MPI_Recv(s, recvsize, MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, this->comm, MPI_STATUS_IGNORE);
				return recvsize;
			} else {
				// Increase the buffer size if needed
				if ((size_t)recvsize > this->buffer.size())
					this->buffer.resize(recvsize);

				// Do the actual receive into the buffer
				MPI_Recv(&this->buffer.front(), recvsize, MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, this->comm, MPI_STATUS_IGNORE);
				this->buffer_start = this->buffer.begin();
				this->buffer_end = this->buffer_start + recvsize;
				// Fall through to the buffered read case
			}
		}

		// If we reach here, either the buffer was not empty when this function was called, or it was just filled

		// Copy the minimum of the total buffered data and the user's requested data
		std::streamsize buffered = std::distance(this->buffer_start, this->buffer_end);
		std::streamsize actual = std::min(buffered, n);

		// Copy from the internal buffer to the user's buffer, then update the
		// valid-data iterators for the internal buffer
		auto buffer_copy_until = this->buffer_start + actual;
		std::uninitialized_copy(this->buffer_start, buffer_copy_until, s);
		this->buffer_start = buffer_copy_until;

		return actual;

		IO_STATS_READ_TIME_END()
	}

	std::streamsize write(const char* s, std::streamsize n) {
		IO_STATS_WRITE_TIME_BEGIN(this->stats)

		// Send directly from the user's buffer
		// Const cast because derpy MPI API
		MPI_Send(const_cast< char* >(s), static_cast< int >(n), MPI_BYTE, this->root, this->tag, this->comm);

		// Update stats
		++this->stats.write_reqs;
		this->stats.write_bytes += n;

		return n;

		IO_STATS_WRITE_TIME_END()
	}

	Stats get_stats() const { return stats; }
	void reset_stats() { stats = Stats(); }

private:
	const MPI_Comm comm;
	const int tag;
	const int root;
	int nsenders; // Not const because MPI_Comm_rank(comm, &nsenders)

	int nreceived;
	std::vector< char > buffer;
	typename std::vector< char >::iterator buffer_start, buffer_end;

	mutable Stats stats;
};

DEFINE_STATS_SERIALIZE(MPIGatherStreamDevice::Stats, stats, stats.read_reqs & stats.write_reqs)

class MPIGatherBufferedStream : public boost::iostreams::stream< MPIGatherStreamDevice > {
public:
	typedef boost::iostreams::stream< MPIGatherStreamDevice > ParentType;
	MPIGatherBufferedStream() : ParentType() {}
	MPIGatherBufferedStream(MPI_Comm comm, int tag, int root) :
		ParentType(MPIGatherStreamDevice(comm, tag, root)) {}
	MPIGatherBufferedStream(MPI_Comm comm, int tag, int root, std::streamsize buffer_size) :
		ParentType(MPIGatherStreamDevice(comm, tag, root), buffer_size) {}

	void open(MPI_Comm comm, int tag, int root) { ParentType::open(MPIGatherStreamDevice(comm, tag, root)); }
	void open(MPI_Comm comm, int tag, int root, std::streamsize buffer_size) { ParentType::open(MPIGatherStreamDevice(comm, tag, root), buffer_size); }

	const MPIGatherStreamDevice & get_mpi_device() const { return *const_cast<MPIGatherBufferedStream&>(*this).component(); }
};

#endif /* MPI_STREAM_HPP_ */
