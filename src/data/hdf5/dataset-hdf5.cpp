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
 * dataset-hdf5.cpp
 *
 *  Created on: Sep 11, 2014
 *      Author: David A. Boyuka II
 */
#ifndef DATASET_HDF5_CPP_
#define DATASET_HDF5_CPP_

#include <hdf5.h>

#include "pique/data/hdf5/dataset-hdf5.hpp"

DatasetHDF5::DatasetHDF5(std::string h5file, std::string varpath) : Dataset(Dataset::Format::HDF5) {
	file_handle = H5Fopen(h5file.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
	if (file_handle == -1) abort();
	dataset_handle = H5Dopen2(file_handle, varpath[0] == '/' ? varpath.c_str() : ("/" + varpath).c_str(), H5P_DEFAULT);
	if (dataset_handle == -1) abort();
	dataset_plist_handle = H5Dget_create_plist(dataset_handle);
	if (dataset_plist_handle == -1) abort();
	dataspace_handle = H5Dget_space(dataset_handle);
	if (dataspace_handle == -1) abort();

	assert(H5Sis_simple(dataspace_handle));
	dataset_rank = H5Sget_simple_extent_ndims(dataspace_handle);

	dataset_dims.resize(dataset_rank);
	H5Sget_simple_extent_dims(dataspace_handle, &dataset_dims.front(), NULL);

	is_chunked = (H5Pget_layout(dataset_plist_handle) == H5D_CHUNKED);
	if (is_chunked) {
		chunk_dims.resize(dataset_rank);
		const int chunk_rank = H5Pget_chunk(dataset_plist_handle, dataset_rank, &chunk_dims.front());
		assert(chunk_rank == dataset_rank);
	}
}

auto DatasetHDF5::get_element_count() const -> dataset_length_t { return H5Sget_select_npoints(this->dataspace_handle); }

Datatypes::IndexableDatatypeID DatasetHDF5::get_datatype() const {
	using DTID = Datatypes::IndexableDatatypeID;
	const hid_t datatype_handle = H5Dget_type(this->dataset_handle);
	if (H5Tequal(datatype_handle, H5T_NATIVE_FLOAT)) return DTID::FLOAT;
	if (H5Tequal(datatype_handle, H5T_NATIVE_DOUBLE)) return DTID::DOUBLE;
	if (H5Tequal(datatype_handle, H5T_NATIVE_UINT8)) return DTID::UINT_8;
	if (H5Tequal(datatype_handle, H5T_NATIVE_INT8)) return DTID::SINT_8;
	if (H5Tequal(datatype_handle, H5T_NATIVE_UINT16)) return DTID::UINT_16;
	if (H5Tequal(datatype_handle, H5T_NATIVE_INT16)) return DTID::SINT_16;
	if (H5Tequal(datatype_handle, H5T_NATIVE_UINT32)) return DTID::UINT_32;
	if (H5Tequal(datatype_handle, H5T_NATIVE_INT32)) return DTID::SINT_32;
	if (H5Tequal(datatype_handle, H5T_NATIVE_UINT64)) return DTID::UINT_64;
	if (H5Tequal(datatype_handle, H5T_NATIVE_INT64)) return DTID::SINT_64;
	return DTID::UNDEFINED;
}

Grid DatasetHDF5::get_grid() const {
	assert(H5Sis_simple(this->dataspace_handle));

	const int ndim = H5Sget_simple_extent_ndims(this->dataspace_handle);
	std::vector<hsize_t> hdims(ndim);

	H5Sget_simple_extent_dims(this->dataspace_handle, &hdims.front(), NULL);

	Grid g(Grid::Linearization::ROW_MAJOR_ORDER);
	g.insert(g.end(), hdims.begin(), hdims.end());
	return g;
}

struct MakeHDF5DatasetStream {
	template<typename datatype_t>
	boost::shared_ptr< DatasetHDF5Stream< datatype_t > > operator()(hid_t dataset_handle, hid_t win_sel_handle) {
		return boost::make_shared< DatasetHDF5Stream< datatype_t > >(dataset_handle, win_sel_handle);
	}
};

boost::shared_ptr< AbstractDatasetStream > DatasetHDF5::open_stream(GridSubset subset) const {
	assert(subset.get_grid() == get_grid());
	hid_t win_sel_handle;

	switch (subset.get_type()) {
	case GridSubset::Type::WHOLE_DOMAIN:
		win_sel_handle = H5Scopy(this->dataspace_handle);
		break;
	case GridSubset::Type::SUBVOLUME:
	{
		std::vector<hsize_t> offsets(subset.get_subvolume_offsets().begin(), subset.get_subvolume_offsets().end());
		std::vector<hsize_t> dims(subset.get_subvolume_dims().begin(), subset.get_subvolume_dims().end());

		win_sel_handle = H5Scopy(this->dataspace_handle);
		H5Sselect_hyperslab(win_sel_handle, H5S_SELECT_SET, &offsets.front(), NULL, &dims.front(), NULL);
		break;
	}
	case GridSubset::Type::LINEARIZED_RANGES:
	{
		if (this->is_chunked && this->dataset_rank > 1) {
			std::cerr << "Error: cannot used LINEARIZED_RANGES GridSubset on an HDF5 dataset that is both chunked and >1D" << std::endl;
			abort();
		}
		// Create a 1D dataspace covering the dataset
		const hsize_t nelem = H5Sget_select_npoints(this->dataspace_handle);
		win_sel_handle = H5Screate_simple(1, &nelem, NULL);

		// Select all the given subranges
		// TODO: Optimization by using H5Sselect_elements when most/all ranges are length 1?
		H5Sselect_none(win_sel_handle);
		for (std::pair< dataset_offset_t, dataset_length_t > range : subset.get_ranges()) {
			const hsize_t offset = range.first, len = range.second;
			H5Sselect_hyperslab(win_sel_handle, H5S_SELECT_OR, &offset, NULL, &len, NULL);
		}
		break;
	}
	}

	return Datatypes::DatatypeIDToCTypeDispatch::template dispatchMatching< boost::shared_ptr< AbstractDatasetStream > >(
		get_datatype(),
		MakeHDF5DatasetStream(),
		nullptr,
		this->dataset_handle, win_sel_handle
	);
}

uint64_t DatasetHDF5::get_natural_partition_count() {
	if (this->is_chunked) {
		uint64_t chunks = 1;
		for (int i = 0; i < this->dataset_rank; ++i)
			chunks *= (this->dataset_dims[i] - 1) / this->chunk_dims[i] + 1; // round up

		return chunks;
	} else {
		return 1;
	}
}

GridSubset DatasetHDF5::get_natural_partition(uint64_t chunk_ind) {
	assert(chunk_ind < get_natural_partition_count());

	if (this->is_chunked) {
		std::vector< uint64_t > chunk_offset(this->dataset_rank);
		for (int i = 0; i < this->dataset_rank; ++i) {
			const uint64_t chunk_count_in_dim = (this->dataset_dims[i] - 1) / this->chunk_dims[i] + 1; // round up
			const uint64_t chunk_ind_in_dim = chunk_ind % chunk_count_in_dim;
			chunk_ind /= chunk_count_in_dim;

			chunk_offset[i] = chunk_ind_in_dim * this->chunk_dims[i];
		}

		std::vector< uint64_t > chunk_size(this->chunk_dims.begin(), this->chunk_dims.end()); // convert from hsize_t vector to uint64_t vector (these are usually the same type, but let's be portable)
		return GridSubset(get_grid(), std::move(chunk_offset), std::move(chunk_size));
	} else {
		return GridSubset(get_grid());
	}
}

#endif /* DATASET_HDF5_CPP_ */
