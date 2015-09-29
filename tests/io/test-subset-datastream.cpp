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
 * test-subset-datastream.cpp
 *
 *  Created on: Aug 28, 2014
 *      Author: David A. Boyuka II
 */

#include <cstdlib>
#include <fstream>
#include <boost/smart_ptr.hpp>

#ifdef HAVE_HDF5
#include <hdf5.h>
#endif

#include "pique/data/grid.hpp"
#include "pique/data/dataset.hpp"
#include "pique/util/datatypes.hpp"

static const std::vector< int > data = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

template<Dataset::Format format>
boost::shared_ptr< Dataset > make_dataset(const std::vector< int > &data, std::string filename, std::string varname);

template<>
boost::shared_ptr< Dataset > make_dataset<Dataset::Format::INMEMORY>(const std::vector< int > &data, std::string filename, std::string varname) {
	return boost::make_shared< InMemoryDataset< int > >(data, Grid{data.size()});
}

template<>
boost::shared_ptr< Dataset > make_dataset<Dataset::Format::RAW>(const std::vector< int > &data, std::string filename, std::string varname) {
	std::fstream fout;
	fout.open(filename, std::ios_base::out);
	fout.write((char*)&data.front(), data.size() * sizeof(int));
	fout.close();

	return boost::make_shared< RawDataset >(filename, Grid{data.size()}, Datatypes::CTypeToDatatypeID< int >::value);
}

#ifdef HAVE_HDF5
template<>
boost::shared_ptr< Dataset > make_dataset<Dataset::Format::HDF5>(const std::vector< int > &data, std::string filename, std::string varname) {
	const hsize_t len = data.size();
	varname = std::string("/") + varname;

	const hid_t h5out = H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
	const hid_t dspace = H5Screate_simple(1, &len, NULL);
	const hid_t dset = H5Dcreate2(h5out, varname.c_str(), H5T_NATIVE_INT32, dspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	const herr_t writeerr = H5Dwrite(dset, H5T_NATIVE_INT32, dspace, dspace, H5P_DEFAULT, &data.front());
	const herr_t dsetcloseerr = H5Dclose(dset);
	const herr_t dspacecloseerr = H5Sclose(dspace);
	const herr_t h5closeerr = H5Fclose(h5out);

	assert(writeerr == 0 && dsetcloseerr == 0 && dspacecloseerr == 0 && h5closeerr == 0);

	return boost::make_shared< DatasetHDF5 >(filename, varname);
}
#endif

template<Dataset::Format format>
void test_dataset(const std::vector< int > &data, std::string filename, std::string varname) {
	boost::shared_ptr< Dataset > dataset = make_dataset<format>(data, filename, varname);

	boost::shared_ptr< AbstractDatasetStream > stream;
	boost::shared_ptr< DatasetStream<int> > intstream;
	std::vector< int > dataout;

	// Test a full read
	stream = dataset->open_stream();
	intstream = boost::dynamic_pointer_cast< DatasetStream< int > >(stream);
	assert(intstream);
	assert(intstream->get_element_count() == data.size());

	dataout.clear();
	intstream->next_fully(data.size(), dataout);

	assert(dataout == data);

	// Test a partial read
	const size_t start = data.size() / 4;
	const size_t end = data.size() / 4 * 3;

	stream = dataset->open_stream(start, end - start);
	intstream = boost::dynamic_pointer_cast< DatasetStream< int > >(stream);
	assert(intstream);
	assert(intstream->get_element_count() == end - start);

	dataout.clear();
	intstream->next_fully(end - start, dataout);

	assert(dataout == std::vector<int>(data.begin() + start, data.begin() + end));
}

int main(int argc, char **argv) {
	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");
	std::string filename = tempdir + "/dataset";
	std::string varname = "myvar";

	test_dataset< Dataset::Format::INMEMORY	>(data, filename, varname);
	test_dataset< Dataset::Format::RAW		>(data, filename, varname);
#ifdef HAVE_HDF5
	test_dataset< Dataset::Format::HDF5		>(data, filename, varname);
#endif
}
