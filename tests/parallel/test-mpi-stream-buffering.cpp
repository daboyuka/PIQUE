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
 * test-mpi-stream-buffering.cpp
 *
 *  Created on: Sep 4, 2014
 *      Author: David A. Boyuka II
 */

#include <vector>

#include <boost/iterator/counting_iterator.hpp>

#include <mpi.h>

#include "pique/parallel/util/mpi-stream.hpp"

constexpr int MPI_BUFSIZE = 1000;
constexpr int WRITE_SIZE = 10001;

int main(int argc, char **argv) {
	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");
	std::string filename = tempdir + "/dataset";

	MPI_Init(&argc, &argv);

	MPI_File mpifile;
	MPI_File_open(MPI_COMM_WORLD, (char*)filename.c_str(), MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &mpifile);

	MPIFileBufferedStream buffer_stream(mpifile, MPI_BUFSIZE);

	// Write WRITE_SIZE bytes, one at a time
	for (int i = 0; i < WRITE_SIZE; ++i)
		buffer_stream << 'X';

	buffer_stream.flush();

	MPIFileStreamDevice &mpidev = *buffer_stream;
	assert(mpidev.get_seek_count() == 0);
	assert(mpidev.get_write_request_count() > 0 && mpidev.get_write_request_count() <= ((WRITE_SIZE - 1) / MPI_BUFSIZE + 1)); // <= because buffer_stream is allowed to write more than MPI_BUFSIZE at a time; in my tests it was MPI_BUFSIZE + 2
	assert(mpidev.get_bytes_written() == WRITE_SIZE);

	buffer_stream.close();
	MPI_File_close(&mpifile);

	MPI_Finalize();
}
