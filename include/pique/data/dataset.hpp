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
 * dataset-stream.hpp
 *
 *  Created on: Aug 22, 2014
 *      Author: David A. Boyuka II
 */
#ifndef DATASET_STREAM_HPP_
#define DATASET_STREAM_HPP_

#include "config.h" // include Autoconf config.h to get HAVE_HDF5

#include <cstdint>
#include <fstream>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/optional.hpp>

#include "pique/util/datatypes.hpp"
#include "pique/data/grid.hpp"
#include "pique/data/grid-subset.hpp"

#include "pique/stats/stats.hpp"

class AbstractDatasetStream;

class Dataset {
public:
	enum struct Format { INMEMORY, RAW, HDF5 };

	using dataset_offset_t = uint64_t;
	using dataset_length_t = dataset_offset_t;

	Dataset(Format format) : format(format) {}
	virtual ~Dataset() throw() {}

	Format get_format() const { return format; };
	virtual dataset_length_t get_element_count() const = 0;
	virtual Datatypes::IndexableDatatypeID get_datatype() const = 0;
	virtual Grid get_grid() const = 0;

	// Return a subclass of DatasetStream<datatype_t>, where datatype_t corresponds to get_datatype()
	virtual boost::shared_ptr< AbstractDatasetStream > open_stream(GridSubset subset) const = 0;

	boost::shared_ptr< AbstractDatasetStream > open_stream() const {
		return this->open_stream(GridSubset(get_grid()));
	}
	boost::shared_ptr< AbstractDatasetStream > open_stream(dataset_offset_t off, dataset_length_t len) const {
		return this->open_stream(GridSubset(get_grid(), off, len));
	}

	virtual uint64_t get_natural_partition_count() { return 1; }
	virtual GridSubset get_natural_partition(uint64_t i) {
		assert(i < get_natural_partition_count());
		return GridSubset(get_grid());
	}

private:
	const Format format;
};

class AbstractDatasetStream {
public:
	using dataset_offset_t = Dataset::dataset_offset_t;
	using dataset_length_t = Dataset::dataset_length_t;

	AbstractDatasetStream(Datatypes::IndexableDatatypeID datatype) : datatype(datatype) {}
	virtual ~AbstractDatasetStream() throw() {}

	virtual dataset_length_t get_element_count() = 0;
	Datatypes::IndexableDatatypeID get_datatype() const { return datatype; }

private:
	const Datatypes::IndexableDatatypeID datatype;
};

template<typename datatype_t>
class DatasetStream : public AbstractDatasetStream {
public:
	using dataset_offset_t = Dataset::dataset_offset_t;
	using dataset_length_t = Dataset::dataset_length_t;

	DatasetStream() : AbstractDatasetStream(Datatypes::CTypeToDatatypeID< datatype_t >::value) {}
	virtual ~DatasetStream() throw() {}

	virtual bool has_next() = 0;
	virtual datatype_t next() = 0;
	virtual dataset_length_t next(dataset_length_t maxcount, std::vector< datatype_t > &out) {
		out.resize(out.size() + maxcount);
		dataset_length_t count = 0;
		for (auto it = out.begin() + out.size(); count < maxcount && has_next(); ++it, ++count)
			*it = next();

		out.resize(out.size() - maxcount + count);
		return count;
	}

	dataset_length_t next_fully(dataset_length_t count, std::vector< datatype_t > &out) {
		dataset_length_t readcount = 0;
		while (readcount < count && this->has_next())
			count += next(count - readcount, out);
		return readcount;
	}

	virtual IOStats get_stats() const = 0;
	virtual void reset_stats() = 0;
};

template<typename datatype_t>
class BufferedDatasetStream : public DatasetStream< datatype_t > {
public:
	using dataset_offset_t = Dataset::dataset_offset_t;
	using dataset_length_t = Dataset::dataset_length_t;
	typedef typename std::vector< datatype_t >::const_iterator buffer_iterator_t;

	BufferedDatasetStream() : done(false), next_it(), end_it() {}
	virtual ~BufferedDatasetStream() throw() {}

	virtual bool has_next() {
		ensure_buffered();
		return next_it != end_it;
	}
	virtual datatype_t next() {
		if (!has_next())
			abort();
		return *next_it++;
	}
	virtual dataset_length_t next(dataset_length_t maxcount, std::vector< datatype_t > &out) {
		if (!has_next())
			return 0;

		const dataset_length_t remaining = std::distance(next_it, end_it);
		const dataset_length_t actualcount = (remaining > maxcount ? maxcount : remaining);

		out.reserve(out.size() + actualcount);
		out.insert(out.end(), next_it, next_it + actualcount);
		next_it += actualcount;
		return actualcount;
	}

	bool get_buffered_data(buffer_iterator_t &begin_it_out, buffer_iterator_t &end_it_out) {
		ensure_buffered();
		begin_it_out = next_it;
		end_it_out = end_it;
		next_it = end_it; // Mark all that data as read

		return (begin_it_out != end_it_out) || !done;
	}

private:
	void ensure_buffered() {
		if (!done && next_it == end_it)
			done = !buffer_more_impl(next_it, end_it);
	}

	// Must return a start/end iterator pair that is valid until the next call to buffer_more_impl
	// Returns true iff there may be more data in the future
	virtual bool buffer_more_impl(buffer_iterator_t &begin_it_out, buffer_iterator_t &end_it_out) = 0;

	bool done;
	buffer_iterator_t next_it, end_it;
};

template<typename datatype_t>
class BlockBufferingDatasetStream : public BufferedDatasetStream< datatype_t > {
public:
	using dataset_offset_t = Dataset::dataset_offset_t;
	using dataset_length_t = Dataset::dataset_length_t;
	static constexpr const dataset_length_t DEFAULT_BUFFER_BLOCKSIZE = (1ULL<<20);

	BlockBufferingDatasetStream(boost::shared_ptr< DatasetStream< datatype_t > > base, dataset_length_t buffer_blocksize = DEFAULT_BUFFER_BLOCKSIZE) :
		buffer_blocksize(buffer_blocksize), base(base)
	{}
	virtual ~BlockBufferingDatasetStream() throw() {}

	virtual dataset_length_t get_element_count() { return base->get_element_count(); }

	virtual IOStats get_stats() const { return base->get_stats(); }
	virtual void reset_stats() { base->reset_stats(); }

private:
	using buffer_iterator_t = typename BufferedDatasetStream< datatype_t >::buffer_iterator_t;
	virtual bool buffer_more_impl(buffer_iterator_t &begin_it_out, buffer_iterator_t &end_it_out) {
		base->next(buffer_blocksize, buffer);
		begin_it_out = buffer.cbegin();
		end_it_out = buffer.cend();
		return base->has_next();
	}

private:
	const dataset_length_t buffer_blocksize;
	boost::shared_ptr< DatasetStream< datatype_t > > base;

	std::vector< datatype_t > buffer;
};

#include "pique/data/raw/dataset-raw.hpp"
#include "pique/data/inmemory/dataset-inmemory.hpp"

#ifdef HAVE_HDF5
#include "pique/data/hdf5/dataset-hdf5.hpp"
#endif

#endif /* DATASET_STREAM_HPP_ */
