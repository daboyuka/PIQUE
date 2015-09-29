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
 * dataset-inmemory.hpp
 *
 *  Created on: Sep 11, 2014
 *      Author: David A. Boyuka II
 */
#ifndef DATASET_INMEMORY_HPP_
#define DATASET_INMEMORY_HPP_

#include "pique/data/dataset.hpp"

template<typename datatype_t>
class InMemoryDatasetStream : public BufferedDatasetStream< datatype_t > {
public:
	using dataset_offset_t = Dataset::dataset_offset_t;
	using dataset_length_t = Dataset::dataset_length_t;
	using data_it_t = typename std::vector< datatype_t >::const_iterator;

	InMemoryDatasetStream(data_it_t begin_it, data_it_t end_it) :
		BufferedDatasetStream< datatype_t >(), begin_it(begin_it), end_it(end_it)
	{}
	virtual ~InMemoryDatasetStream() throw() {}

	virtual dataset_length_t get_element_count() { return std::distance(begin_it, end_it); }

	virtual IOStats get_stats() const { return stats; }
	virtual void reset_stats() { stats = IOStats(); }

private:
	using buffer_iterator_t = typename BufferedDatasetStream< datatype_t >::buffer_iterator_t;
	virtual bool buffer_more_impl(buffer_iterator_t &begin_it_out, buffer_iterator_t &end_it_out) {
		begin_it_out = begin_it;
		end_it_out = end_it;
		return false; // There won't be any further data to buffer after this
	}

private:
	const data_it_t begin_it, end_it;

	mutable IOStats stats;
};

template<typename datatype_t>
class InMemoryDataset : public Dataset {
public:
	using dataset_offset_t = Dataset::dataset_offset_t;
	using dataset_length_t = Dataset::dataset_length_t;

	InMemoryDataset(std::vector< datatype_t > data, Grid grid) : Dataset(Dataset::Format::INMEMORY), data(std::move(data)), grid(std::move(grid)) {
		assert(this->grid.get_npoints() == this->data.size());
	}
	virtual ~InMemoryDataset() throw() {}

	virtual dataset_length_t get_element_count() const { return data.size(); }
	virtual Datatypes::IndexableDatatypeID get_datatype() const { return Datatypes::CTypeToDatatypeID< datatype_t >::value; }
	virtual Grid get_grid() const { return grid; }

	virtual boost::shared_ptr< AbstractDatasetStream > open_stream(GridSubset subset) const {
		switch (subset.get_type()) {
		case GridSubset::Type::WHOLE_DOMAIN:
			return boost::make_shared< InMemoryDatasetStream< datatype_t > >(data.begin(), data.end());
		case GridSubset::Type::LINEARIZED_RANGES:
		{
			assert(subset.get_ranges().size() == 1 /* only 1 range is allowed */);
			const dataset_offset_t begin_off = subset.get_ranges().front().first;
			const dataset_offset_t end_off = begin_off + subset.get_ranges().front().second;
			return boost::make_shared< InMemoryDatasetStream< datatype_t > >(data.begin() + begin_off, data.begin() + end_off);
		}
		default:
			std::cerr << "Unsupported grid subset selection for InMemoryDataset: " << (int)subset.get_type() << std::endl;
			abort();
			return nullptr;
		}

		return boost::make_shared< InMemoryDatasetStream< datatype_t > >(data.begin(), data.end());
	}
private:
	const std::vector< datatype_t > data;
	const Grid grid;
};

#endif /* DATASET_INMEMORY_HPP_ */
