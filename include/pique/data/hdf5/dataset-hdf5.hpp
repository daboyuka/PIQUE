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
 * dataset-hdf5.hpp
 *
 *  Created on: Sep 11, 2014
 *      Author: David A. Boyuka II
 */
#ifndef DATASET_HDF5_HPP_
#define DATASET_HDF5_HPP_

#include <hdf5.h>

#include "pique/data/dataset.hpp"

template<typename datatype_t>
class DatasetHDF5Stream : public BufferedDatasetStream< datatype_t > {
public:
	using dataset_offset_t = Dataset::dataset_offset_t;
	using dataset_length_t = Dataset::dataset_length_t;

	DatasetHDF5Stream(hid_t dataset_handle, hid_t win_sel_handle) :
		BufferedDatasetStream< datatype_t >(), dataset_handle(dataset_handle), win_sel_handle(win_sel_handle)
	{
		using dataset_length_t = Dataset::dataset_length_t;

		const hid_t datatype_handle = H5Dget_type(dataset_handle);
		const hsize_t buffer_size = H5Sget_select_npoints(win_sel_handle);
		buffer.resize(buffer_size);

		// TODO: Use kD memory dataspace to get better performance (http://www.hdfgroup.org/HDF5/faq/perfissues.html)
		const hid_t buffer_sel = H5Screate_simple(1, &buffer_size, NULL);

		IO_STATS_READ_TIME_BEGIN(stats)
			const herr_t readerr = H5Dread(dataset_handle, datatype_handle, buffer_sel, win_sel_handle, H5P_DEFAULT, &buffer.front());
			assert(readerr >= 0); // TODO: better error handling
		IO_STATS_READ_TIME_END()

		stats.read_bytes += buffer_size;
		stats.read_seeks += 1; // TODO: incorrect for subvolumes and multiple linearized ranges

		H5Tclose(datatype_handle);
		H5Sclose(buffer_sel);
	}
	virtual ~DatasetHDF5Stream() throw() {
		H5Sclose(win_sel_handle);
	}

	virtual dataset_length_t get_element_count() { return H5Sget_select_npoints(win_sel_handle); }

	virtual IOStats get_stats() const { return stats; }
	virtual void reset_stats() { stats = IOStats(); }

private:
	using buffer_iterator_t = typename BufferedDatasetStream< datatype_t >::buffer_iterator_t;
	virtual bool buffer_more_impl(buffer_iterator_t &begin_it_out, buffer_iterator_t &end_it_out) {
		begin_it_out = buffer.cbegin();
		end_it_out = buffer.cend();
		return false; // There won't be any further data to buffer after this
	}

private:
	hid_t dataset_handle;
	hid_t win_sel_handle;
	std::vector< datatype_t > buffer;

	mutable IOStats stats;
};

class DatasetHDF5 : public Dataset {
public:
	using dataset_offset_t = Dataset::dataset_offset_t;
	using dataset_length_t = Dataset::dataset_length_t;

	DatasetHDF5(std::string h5file, std::string varpath);
	virtual ~DatasetHDF5() throw() {
		H5Sclose(dataspace_handle);
		H5Pclose(dataset_plist_handle);
		H5Dclose(dataset_handle);
		H5Fclose(file_handle);
	}

	virtual dataset_length_t get_element_count() const;
	virtual Datatypes::IndexableDatatypeID get_datatype() const;
	virtual Grid get_grid() const;

	virtual boost::shared_ptr< AbstractDatasetStream > open_stream(GridSubset subset) const;

	virtual uint64_t get_natural_partition_count();
	virtual GridSubset get_natural_partition(uint64_t chunk_ind);

private:
	hid_t file_handle;
	hid_t dataset_handle;
	hid_t dataset_plist_handle;
	hid_t dataspace_handle;

	int dataset_rank;
	std::vector<hsize_t> dataset_dims;

	bool is_chunked;
	std::vector<hsize_t> chunk_dims;
};

#endif /* DATASET_HDF5_HPP_ */
