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
 * serializable-chunk.hpp
 *
 *  Created on: May 7, 2014
 *      Author: David A. Boyuka II
 */
#ifndef SERIALIZABLE_CHUNK_HPP_
#define SERIALIZABLE_CHUNK_HPP_

#include <iostream>
#include <fstream>
#include <functional>

#include <boost/none.hpp>
#include <boost/optional.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/null.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/filter/counter.hpp>
#include <boost/iostreams/filtering_stream.hpp>

#include "pique/util/fixed-archive.hpp"
#include "pique/stats/stats.hpp"

class byte_counter_streambuf : public std::streambuf {
public:
	virtual ~byte_counter_streambuf() {}

    std::streamsize get_byte_count() const { return bytes; }

	virtual std::streamsize xsputn(const char_type*, std::streamsize n) { bytes += n; return n; }
	virtual int_type overflow(int_type c = traits_type::eof()) { bytes += (c != traits_type::eof() ? 1 : 0); return c; }

private:
	std::streamsize bytes{0};
};

class measuring_stream : public std::ostream {
public:
	measuring_stream() {
		this->rdbuf(&byte_counter);
	}
	virtual ~measuring_stream() {
		// clear the streambuf, since it will be destroyed before the superclass
		// destructor is called, and we need to ensure it isn't used there
		this->rdbuf(nullptr);
	}

	std::streamsize get_byte_count() { return byte_counter.get_byte_count(); }
private:
	byte_counter_streambuf byte_counter;
};

template<typename SerializableT>
inline std::streamsize measure_serializable(SerializableT &obj, unsigned int flags = 0) {
	namespace io = boost::iostreams;

	measuring_stream mstream;
	boost::archive::simple_binary_oarchive binary_write(mstream, flags);
	binary_write << obj;

	return mstream.get_byte_count();
}

template<typename SerializableT, typename offset_t = std::streampos, typename length_t = std::streampos>
class SerializableChunk : public boost::optional<SerializableT> {
public:
	SerializableChunk(
			std::function< offset_t() > offset_fn,
			std::function< std::ios::seekdir() > seekdir_fn,
			bool write_boost_header = false) :
		offset_fn(offset_fn), seekdir_fn(seekdir_fn), write_boost_header(write_boost_header)
	{}

	void cache(std::istream &in) {
		if (!this->is_initialized()) read(in);
	}
	std::streamsize read(std::istream &in) {
		*this = SerializableT();
		const offset_t offset = offset_fn();

		{
			TimeBlock iotimer = stats.open_read_time_block();
			if (in.tellg() != offset) {
				in.seekg(offset, seekdir_fn()); // Only seek when we need to, to avoid spurious flushes
				++stats.read_seeks;
			}
			boost::archive::simple_binary_iarchive binary_read(in, get_archive_flags());
			binary_read >> **this;
		}

		std::streamsize bytes_read = in.tellg() - offset;
		stats.read_bytes += bytes_read;
		return bytes_read;
	}
	std::streamsize write(std::ostream &out) const {
		assert(this->is_initialized());
		const std::streampos offset = offset_fn();

		{
			TimeBlock iotimer = stats.open_write_time_block();
			if (out.tellp() != offset) {
				out.seekp(offset, seekdir_fn()); // Only seek when we need to, to avoid spurious flushes
				++stats.write_seeks;
			}
			boost::archive::simple_binary_oarchive binary_write(out, get_archive_flags());
			binary_write << **this;
		}

		std::streamsize bytes_written = out.tellp() - offset;
		stats.write_bytes += bytes_written;
		return bytes_written;
	}
	std::streamsize measure() const {
		return measure_serializable(**this, get_archive_flags());
	}

	std::streamsize read(std::vector< char > buffer) {
		boost::iostreams::basic_array_source<char> buffer_array_device(&buffer.front(), buffer.size());
		boost::iostreams::stream< boost::iostreams::basic_array_source<char> > buffer_stream(buffer_array_device);
		return read(buffer_stream);
	}
	std::streamsize write(std::vector< char > buffer, bool preallocated = true) const {
		if (!preallocated)
			buffer.resize(measure());

		boost::iostreams::basic_array_sink<char> buffer_array_device(&buffer.front(), buffer.size());
		boost::iostreams::stream< boost::iostreams::basic_array_sink<char> > buffer_stream(buffer_array_device);
		return write(buffer_stream);
	}

	// Required because operator= must return a reference to an object of the same type as the enclosing class
	// (the inherited operator= from boost::optional returns a boost::optional, which isn't derived enough)
	SerializableChunk& operator=( SerializableT const& v ) { boost::optional<SerializableT>::operator=(v); return *this; }
	SerializableChunk& operator=( boost::none_t ) { boost::optional<SerializableT>::operator=(boost::none); return *this; }

	IOStats get_stats() const { return stats; }
	void reset_stats() { stats = IOStats(); }

private:
	int get_archive_flags() const {
		return (write_boost_header ?
				0 :
				boost::archive::no_header);
	}

private:
	const std::function< offset_t() > offset_fn;
	const std::function< std::ios::seekdir() > seekdir_fn;
	const bool write_boost_header;
	mutable IOStats stats;
};



template<typename SerializableT>
class FileBoundSerializableChunk : public SerializableChunk< SerializableT, std::streampos, std::streamsize > {
public:
	using ParentType = SerializableChunk< SerializableT, std::streampos, std::streamsize >;
	FileBoundSerializableChunk(std::function<std::fstream&()> file_fn, std::function<std::streampos()> offset_fn, std::function<std::ios::seekdir()> seekdir_fn, bool write_boost_header = false) :
		ParentType(offset_fn, seekdir_fn, write_boost_header),
		file_fn(file_fn) {}

	void cache() {
		this->cache(file_fn());
	}
	std::streamsize read() {
		return this->read(file_fn());
	}
	std::streamsize write() const {
		return this->write(file_fn());
	}

	// Required because operator= must return a reference to an object of the same type as the enclosing class
	// (the inherited operator= from boost::optional returns a boost::optional, which isn't derived enough)
	FileBoundSerializableChunk& operator=( SerializableT const& v ) { ParentType::operator=(v); return *this; }
	FileBoundSerializableChunk& operator=( boost::none_t ) { ParentType::operator=(boost::none); return *this; }

private:
	const std::function<std::fstream&()> file_fn;
};

#endif /* SERIALIZABLE_CHUNK_HPP_ */
