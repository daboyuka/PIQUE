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
 * test-mpi-stream-device.cpp
 *
 *  Created on: Sep 3, 2014
 *      Author: David A. Boyuka II
 */

#include <vector>

#include <boost/iterator/counting_iterator.hpp>

#include <mpi.h>

#include "pique/parallel/util/mpi-stream.hpp"

int main(int argc, char **argv) {
	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");
	std::string filename = tempdir + "/dataset";

	MPI_Init(&argc, &argv);

	MPI_File mpifile;
	MPI_File_open(MPI_COMM_WORLD, (char*)filename.c_str(), MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &mpifile);

	std::string data(boost::counting_iterator<char>(0), boost::counting_iterator<char>(100));
	MPI_File_write(mpifile, &data.front(), data.size(), MPI_BYTE, MPI_STATUS_IGNORE);

	MPI_File_set_view(mpifile, 10, MPI_BYTE, MPI_BYTE, "native", MPI_INFO_NULL);
	MPIFileStreamDevice mpidev(mpifile);
	boost::iostreams::stream< MPIFileStreamDevice > buffer_stream(mpidev);

	std::string outdata = "abcdefghijklmnopqrst";
	std::string indatabefore(20, 0);
	std::string indataafter(20, 0);
	assert(outdata.length() == 20);

	buffer_stream.read(&indatabefore[0], 20);

	for (int i = 10; i < 30; i++)
		assert(data[i] == indatabefore[i - 10]);

	buffer_stream.seekg(0, std::ios_base::beg);
	buffer_stream.write(outdata.c_str(), 20);
	buffer_stream.seekg(0, std::ios_base::beg);
	buffer_stream.read(&indataafter[0], 20);

	// Check via loop to ensure nulls (\0) don't mess up the comparison
	for (size_t i = 0; i < outdata.length(); i++)
		assert(indataafter[i] == outdata[i]);

	MPI_File_set_view(mpifile, 0, MPI_BYTE, MPI_BYTE, "native", MPI_INFO_NULL);
	MPIFileStreamDevice mpidev2(mpifile);
	boost::iostreams::stream< MPIFileStreamDevice > buffer_stream2(mpidev2);

	std::string data2(100, 0);
	buffer_stream2.read(&data2[0], 100);

	for (int i = 0; i < 100; i++) {
		if (i < 10 || i >= 30)
			assert(data2[i] == i);
		else
			assert(data2[i] == outdata[i - 10]);
	}

	MPI_File_close(&mpifile);

	MPI_Finalize();
}
