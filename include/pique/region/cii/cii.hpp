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
 * cii.hpp
 *
 *  Created on: Feb 27, 2014
 *      Author: David A. Boyuka II
 */

#pragma once

#include <iostream>
#include <cstdint>
#include <boost/smart_ptr.hpp>
#include <boost/limits.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/serialization/vector.hpp>

// ridcompress library
#include <patchedframeofreference.h>

#include "pique/region/region-encoding.hpp"
#include "pique/util/fixed-archive.hpp"

// CIIRegionEncoding, a concrete subclass of RegionEncoding utilizing a compressed (PForDelta) inverted index
class CIIRegionEncoding : public RegionEncoding {
public:
	static constexpr RegionEncoding::Type TYPE = RegionEncoding::Type::CII;
	typedef uint32_t rid_t;

public:
	CIIRegionEncoding() :
		is_compressed(false),
		is_inverted(false),
		domain_size(0)
	{}
	CIIRegionEncoding(uint64_t domain_size) :
		is_compressed(false),
		is_inverted(false),
		domain_size(static_cast<rid_t>(domain_size))
	{}
	CIIRegionEncoding(uint64_t nelem, bool filled) :
		is_compressed(false),
		is_inverted(filled),
		domain_size(static_cast<rid_t>(nelem))
	{
		assert(nelem <= (uint64_t)std::numeric_limits<rid_t>::max());
	}
	CIIRegionEncoding(const CIIRegionEncoding &) = default; // Explicitly allow copy construction
	CIIRegionEncoding(CIIRegionEncoding &&) noexcept = default; // Explicitly allow move construction
	virtual ~CIIRegionEncoding() {}

	CIIRegionEncoding & operator=(CIIRegionEncoding &&) = default; // Explicitly allow move assignment

	virtual RegionEncoding::Type get_type() const { return RegionEncoding::Type::CII; }

    virtual size_t get_size_in_bytes() const;
    virtual void dump() const;

    virtual uint64_t get_domain_size() const { return domain_size; }
	virtual uint64_t get_element_count() const;
	virtual void convert_to_rids(std::vector<uint32_t> &out, bool sorted = false, bool preserve_self = true);

    bool is_compressed_form() const { return this->is_compressed; }

    void compress();
    void decompress();

	virtual void save_to_stream(std::ostream &out) { boost::archive::simple_binary_oarchive(out, boost::archive::no_header) << *this; }
	virtual void load_from_stream(std::istream &in) { boost::archive::simple_binary_iarchive(in, boost::archive::no_header) >> *this; }

	virtual bool operator==(const RegionEncoding &other) const;

private:
    friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	enum : char { COMPRESSED_FLAG = 1, INVERTED_FLAG = 2 };

    	char flags = 0;
    	if (Archive::is_saving::value) {
    		flags |= (this->is_compressed ? COMPRESSED_FLAG : 0);
    		flags |= (this->is_inverted ? INVERTED_FLAG : 0);
    	}
    	ar & flags;
    	if (Archive::is_loading::value) {
        	this->is_compressed = (flags & COMPRESSED_FLAG);
        	this->is_inverted = (flags & INVERTED_FLAG);
    	}

    	ar & this->domain_size;
    	if (is_compressed)	ar & this->cii;
    	else				ar & this->ii;
    }

private:
    static const size_t CII_CHUNK_SIZE;

    bool is_compressed;
    bool is_inverted;
    rid_t domain_size;
    std::vector<char> cii;
    std::vector<rid_t> ii;

    friend class CIIRegionEncoder;
    friend class CIISetOperations;
    friend class CIISetOperationsNAry;
    friend class BaseProgressiveCIIDecoder;
    friend class ProgressiveCIIDecoder;
};

template<> class RegionEncoding::TypeToClass<RegionEncoding::Type::CII> { typedef CIIRegionEncoding REClass; };

BOOST_CLASS_IMPLEMENTATION(CIIRegionEncoding, boost::serialization::object_serializable) // no version information serialized
