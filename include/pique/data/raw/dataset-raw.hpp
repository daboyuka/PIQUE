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
 * dataset-raw.hpp
 *
 *  Created on: Sep 11, 2014
 *      Author: David A. Boyuka II
 */
#ifndef DATASET_RAW_HPP_
#define DATASET_RAW_HPP_

#include "pique/data/dataset.hpp"

template<typename datatype_t>
class RawDatasetStream : public BufferedDatasetStream< datatype_t > {
public:
	using dataset_offset_t = Dataset::dataset_offset_t;
	using dataset_length_t = Dataset::dataset_length_t;
	using data_it_t = typename std::vector< datatype_t >::const_iterator;

	RawDatasetStream(std::string filename, dataset_offset_t seek, dataset_length_t length) :
		BufferedDatasetStream< datatype_t >(), data(length)
	{
		const uint64_t bytestoread = length * sizeof(datatype_t);
		const uint64_t bytestoseek = seek * sizeof(datatype_t);

		std::ifstream fin;
		fin.open(filename, std::ios_base::in);
		if (bytestoseek > 0) {
			fin.seekg(bytestoseek, std::ios_base::beg);
			++stats.read_seeks;
		}

		IO_STATS_READ_TIME_BEGIN(stats)
		fin.read((char*)&data.front(), bytestoread);
		IO_STATS_READ_TIME_END()

		const uint64_t bytesread = fin.gcount();
		stats.read_bytes += bytesread;

		assert(bytesread == bytestoread);
		fin.close();
		assert(!fin.fail());
	}
	virtual ~RawDatasetStream() throw() {}

	virtual dataset_length_t get_element_count() { return data.size(); }

	virtual IOStats get_stats() const { return stats; }
	virtual void reset_stats() { stats = IOStats(); }

private:
	using buffer_iterator_t = typename BufferedDatasetStream< datatype_t >::buffer_iterator_t;
	virtual bool buffer_more_impl(buffer_iterator_t &begin_it_out, buffer_iterator_t &end_it_out) {
		begin_it_out = data.cbegin();
		end_it_out = data.cend();
		return false; // There won't be any further data to buffer after this
	}

private:
	std::vector< datatype_t > data;

	mutable IOStats stats;
};

class RawDataset : public Dataset {
public:
	using dataset_offset_t = Dataset::dataset_offset_t;
	using dataset_length_t = Dataset::dataset_length_t;

	RawDataset(std::string filename, Grid grid, Datatypes::IndexableDatatypeID dtid) : Dataset(Dataset::Format::RAW), filename(std::move(filename)), grid(std::move(grid)), dtid(dtid) {}
	virtual ~RawDataset() throw() {}

	virtual dataset_length_t get_element_count() const { return grid.get_npoints(); }
	virtual Datatypes::IndexableDatatypeID get_datatype() const { return dtid; }
	virtual Grid get_grid() const { return grid; }

	virtual boost::shared_ptr< AbstractDatasetStream > open_stream(GridSubset subset) const {
		switch (subset.get_type()) {
		case GridSubset::Type::WHOLE_DOMAIN:
			return make_stream(0, grid.get_npoints());
		case GridSubset::Type::LINEARIZED_RANGES:
			assert(subset.get_ranges().size() == 1 /* only 1 range is allowed */);
			return make_stream(subset.get_ranges().front().first, subset.get_ranges().front().second);
		default:
			std::cerr << "Unsupported grid subset selection for RawDataset: " << (int)subset.get_type() << std::endl;
			abort();
			return nullptr;
		}
	}

private:
	struct MakeStreamHelper {
		template<typename datatype_t>
		boost::shared_ptr< AbstractDatasetStream > operator()(std::string filename, dataset_offset_t seek, dataset_length_t len) {
			return boost::make_shared< RawDatasetStream< datatype_t > >(filename, seek, len);
		}
	};
	boost::shared_ptr< AbstractDatasetStream > make_stream(dataset_offset_t seek, dataset_length_t len) const {
		return Datatypes::DatatypeIDToCTypeDispatch::template dispatchMatching< boost::shared_ptr< AbstractDatasetStream > >(
				this->dtid,
				MakeStreamHelper(),
				nullptr,
				this->filename, seek, len
		);
	}

private:
	const std::string filename;
	const Grid grid;
	const Datatypes::IndexableDatatypeID dtid;
};

#endif /* DATASET_RAW_HPP_ */
