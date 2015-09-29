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
 * bitmap.hpp
 *
 *  Created on: Jun 17, 2014
 *      Author: David A. Boyuka II
 */

#pragma once

#include <iostream>
#include <cstdint>
#include <boost/smart_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/dynamic_bitset.hpp>

#include "pique/region/region-encoding.hpp"

#include "pique/util/fixed-archive.hpp"

// BitmapRegionEncoding, a concrete subclass of RegionEncoding utilizing a uncompressed bitmap encoding
class BitmapRegionEncoding : public RegionEncoding {
public:
	static constexpr RegionEncoding::Type TYPE = RegionEncoding::Type::UNCOMPRESSED_BITMAP;

	typedef uint64_t block_t;
	static const int BITS_PER_BLOCK = std::numeric_limits<block_t>::digits;

private:
	static void allocate_nbits(BitmapRegionEncoding &bitmap, uint64_t nbits, block_t pattern = 0) {
		uint64_t nblocks = (nbits + BITS_PER_BLOCK - 1) / BITS_PER_BLOCK;

		bitmap.domain_size = nbits;
		bitmap.bits = std::vector< block_t >(nblocks, pattern);
	}

	static constexpr uint64_t bits_to_blocks(uint64_t nbits) { return (nbits + BITS_PER_BLOCK - 1) / BITS_PER_BLOCK; }

public:
	BitmapRegionEncoding() : RegionEncoding(0), bits() {}
	BitmapRegionEncoding(uint64_t nelem, bool filled) :
		RegionEncoding(nelem),
		bits(bits_to_blocks(nelem), filled ? ~(block_t)0 : (block_t)0)
	{}

	BitmapRegionEncoding(uint64_t nbits, const std::vector< block_t > &bits) : RegionEncoding(nbits), bits(bits.begin(), bits.end()) {}
	BitmapRegionEncoding(uint64_t nbits, std::vector< block_t > &&bits) : RegionEncoding(nbits), bits(std::move(bits)) {}
	explicit BitmapRegionEncoding(BitmapRegionEncoding &) = default; // Allow (explicit-only) copy construction
	BitmapRegionEncoding(BitmapRegionEncoding &&) noexcept = default; // Allow move construction
	virtual ~BitmapRegionEncoding() {}

	BitmapRegionEncoding & operator=(BitmapRegionEncoding &&) = default; // Explicitly allow move assignment

	virtual RegionEncoding::Type get_type() const { return RegionEncoding::Type::UNCOMPRESSED_BITMAP; }

    virtual size_t get_size_in_bytes() const { return this->domain_size / std::numeric_limits<uint8_t>::digits; }
    virtual void dump() const;

	virtual uint64_t get_element_count() const;
	virtual void convert_to_rids(std::vector<uint32_t> &out, bool sorted = false, bool preserve_self = true);

	void to_bitset(boost::dynamic_bitset<block_t> &out);
	void zero(); // set all bits to 0
	void fill(); // set all bits to 1

	virtual void save_to_stream(std::ostream &out) { boost::archive::simple_binary_oarchive(out, boost::archive::no_header) << *this; }
	virtual void load_from_stream(std::istream &in) { boost::archive::simple_binary_iarchive(in, boost::archive::no_header) >> *this; }

	virtual bool operator==(const RegionEncoding &other) const;

private:
    friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	ar & this->domain_size;
    	ar & this->bits; // TODO: Smarter serialization? This wastes several bytes (for recording the length again, and storing up to 7 bytes padding)
    }

private:
    std::vector<block_t> bits;

    friend class BitmapRegionEncoder;
    friend class BitmapSetOperations;
    template<int ndim2> friend class CBLQToBitmapConverter;
    template<int ndim2> friend class CBLQToBitmapDFConverter;
};

template<> class RegionEncoding::TypeToClass<RegionEncoding::Type::UNCOMPRESSED_BITMAP> { typedef BitmapRegionEncoding REClass; };

BOOST_CLASS_IMPLEMENTATION(BitmapRegionEncoding, boost::serialization::object_serializable) // no version information serialized
