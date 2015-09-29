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
#include <vector>
#include <fstream>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/none.hpp>

#ifdef HAVE_HDF5
#include <hdf5.h>
#endif

#include "pique/data/grid.hpp"
#include "pique/data/dataset.hpp"
#include "pique/indexing/index-builder.hpp"
#include "pique/region/region-encoding.hpp"
#include "pique/region/ii/ii.hpp"
#include "pique/region/ii/ii-encode.hpp"
#include "pique/io/database.hpp"
#include "pique/util/datatypes.hpp"
#include "pique/util/universal-value.hpp"

static const std::vector< int > data = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

template<Dataset::Format format>
static boost::shared_ptr< Dataset > write_dataset(const std::vector< int > &data, std::string metafilename, std::string filename, std::string varname);

template<>
boost::shared_ptr< Dataset > write_dataset<Dataset::Format::INMEMORY>(const std::vector< int > &data, std::string metafilename, std::string filename, std::string varname) {
	abort();
}

template<>
boost::shared_ptr< Dataset > write_dataset<Dataset::Format::RAW>(const std::vector< int > &data, std::string metafilename, std::string filename, std::string varname) {
	std::fstream fout;
	fout.open(filename, std::ios_base::out);
	fout.write((char*)&data.front(), data.size() * sizeof(int));
	fout.close();
	assert(!fout.fail());

	fout.open(metafilename, std::ios_base::out);
	fout << "RAW" << " " << filename << std::endl;
	fout << "int" << " " << "C" << " " << data.size() << std::endl; // 1D array of 16 ints in C order
	fout.close();
	assert(!fout.fail());

	return boost::make_shared< RawDataset >(filename, Grid{data.size()}, Datatypes::CTypeToDatatypeID< int >::value);
}

#ifdef HAVE_HDF5
template<>
boost::shared_ptr< Dataset > write_dataset<Dataset::Format::HDF5>(const std::vector< int > &data, std::string metafilename, std::string filename, std::string varname) {
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

	std::fstream fout;
	fout.open(metafilename, std::ios_base::out);
	fout << "HDF5" << " " << filename << std::endl;
	fout << varname << std::endl;
	fout.close();
	assert(!fout.fail());

	return boost::make_shared< DatasetHDF5 >(filename, varname);
}
#endif

template<Dataset::Format format>
void test_dataset(const std::vector< int > &data, std::string metafilename, std::string filename, std::string varname) {
	typedef SigbitsBinningSpecification< int > BinningSpecType;

	write_dataset<format>(data, metafilename, filename, varname);

	Database db;
	boost::shared_ptr< DataVariable > dbvar = boost::make_shared< DataVariable >(varname, metafilename, boost::none);
	int varid = db.add_variable(dbvar);

	boost::shared_ptr< Dataset > dataset = db.get_variable(varid)->open_dataset();

	IndexBuilder< int, IIRegionEncoder, BinningSpecType > indexbuilder(IIRegionEncoderConfig(), boost::make_shared< BinningSpecType >(31));
	boost::shared_ptr< BinnedIndex > index = indexbuilder.build_index(*dataset);

	std::vector< UniversalValue > expected_bin_keys{ 0U, 1U, 2U, 3U, 4U, 5U, 6U, 7U };
	assert(index->get_all_bin_keys() == expected_bin_keys);

	for (unsigned int i = 0; i < 8; i++) {
		std::vector< IIRegionEncoding::rid_t > expected_rids = { 2 * i, 2 * i + 1 };
		assert(*index->get_region(i) == IIRegionEncoding(expected_rids.begin(), expected_rids.end(), data.size()));
	}
}

int main(int argc, char **argv) {
	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");
	std::string filename = tempdir + "/dataset";
	std::string varname = "myvar";
	std::string metafilename = tempdir + "/dataset.meta";

	test_dataset< Dataset::Format::RAW		>(data, metafilename, filename, varname);
#ifdef HAVE_HDF5
	test_dataset< Dataset::Format::HDF5		>(data, metafilename, filename, varname);
#endif
}
