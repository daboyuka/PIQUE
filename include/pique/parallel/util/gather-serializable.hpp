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
 * gather-serializable.hpp
 *
 *  Created on: Jan 2, 2015
 *      Author: David A. Boyuka II
 */
#ifndef GATHER_SERIALIZABLE_HPP_
#define GATHER_SERIALIZABLE_HPP_

#include <mpi.h>
#include <vector>

#include <boost/optional.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>

#include "pique/util/fixed-archive.hpp"
#include "pique/util/serializable-chunk.hpp"

template<typename SerializableT>
boost::optional< std::vector< SerializableT > > gather_serializables(SerializableT local_serializable, int master_rank = 0) {
	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	const bool is_master = (rank == master_rank);

	// Compute the size of the local serializable
	int local_serializable_size = (int)measure_serializable(local_serializable, boost::archive::no_header);

	// Allocate a memory buffer for (de)serialization and a displacment array (for master only)
	std::vector< int > serializable_sizes(size, 0); // Only used by master
	std::vector< int > serializable_displs(size, 0); // Only used by master
	std::vector< char > buffer;

	// First, gather all serializable sizes to master so displacements can be computed
	MPI_Gather(&local_serializable_size, 1, MPI_INT, &serializable_sizes.front(), 1, MPI_INT, master_rank, MPI_COMM_WORLD);

	// Next, compute displacements and total size if master
	// Also, if master, allocate enough memory for all serializables; if not, allocate just enough for local
	if (is_master) {
		int serializables_totalsize = 0;
		for (int i = 0; i < size; ++i) {
			serializable_displs[i] = serializables_totalsize;
			serializables_totalsize += serializable_sizes[i];
		}

		buffer.resize(serializables_totalsize);
	} else {
		buffer.resize(local_serializable_size);
	}

	// Wrap the local buffer in a stream
	boost::iostreams::basic_array<char> buffer_array(&buffer.front(), buffer.size());
	boost::iostreams::stream< boost::iostreams::basic_array<char> > buffer_stream(buffer_array);

	// Serialize the local serializable to the buffer. For master, this will be serializing
	// to the correct location in the full buffer (offset 0); for non-master, this will
	// be filling the local buffer only
	boost::archive::simple_binary_oarchive(buffer_stream, boost::archive::no_header) << local_serializable;

	// Gather all local serializables to master (use MPI_IN_PLACE because we've already serialized the
	// master's serializable directly into the correct spot in the master's buffer)
	MPI_Gatherv((is_master ? MPI_IN_PLACE : &buffer.front()), local_serializable_size, MPI_BYTE,
                &buffer.front(), &serializable_sizes.front(), &serializable_displs.front(), MPI_BYTE,
                master_rank, MPI_COMM_WORLD);

	// Finally, if on the master, deserialize and collect all serializables
	if (is_master) {
		buffer_stream.seekg(0, std::ios::beg);
		boost::archive::simple_binary_iarchive inarchive(buffer_stream, boost::archive::no_header);

		std::vector< SerializableT > all_serializables(size, SerializableT());
		for (int i = 0; i < size; ++i) {
			inarchive >> all_serializables[i];
			assert(buffer_stream.tellg() == serializable_displs[i] + serializable_sizes[i]); // Ensure each object deserializes to the expected length
		}

		return boost::make_optional(all_serializables);
	} else {
		return boost::none;
	}
}


#endif /* GATHER_SERIALIZABLE_HPP_ */
