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
 * test-mpi-gather-stream.cpp
 *
 *  Created on: Sep 29, 2014
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

	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	assert(size <= 256);

	MPIGatherBufferedStream stream;
	stream.open(MPI_COMM_WORLD, 123, 0);

	if (rank == 0) {
		std::vector<char> inbuf(0, size * (size - 1) / 2);
		auto in_it = inbuf.begin();

		std::streamsize readsize;
		while (stream.read(&*in_it, size) && (readsize = stream.gcount()) > 0)
			in_it += readsize;

		assert(in_it == inbuf.end());

		in_it = inbuf.begin();
		while (in_it != inbuf.end()) {
			char first = *in_it++;
			for (int i = 0; i < first - 1; ++i)
				assert(*in_it++ == first);
		}
	} else {
		std::vector<char> outbuf(rank, (rank % 256));
		stream.write(&outbuf.front(), outbuf.size());
	}

	MPI_Finalize();
}
