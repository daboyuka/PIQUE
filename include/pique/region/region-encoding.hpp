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
 * region-encoding.hpp
 *
 *  Created on: Feb 3, 2014
 *      Author: David A. Boyuka II
 */
#ifndef REGION_ENCODING_HPP_
#define REGION_ENCODING_HPP_

#include <iostream>
#include <typeindex>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/optional.hpp>

// RegionEncoding, which describes a subset of a large set elements
class RegionEncoding {
public:
	enum struct RegionUniformity : char {
		EMPTY = 0,
		FILLED = 1,
		MIXED = 2,
	};

	static RegionUniformity complement(RegionUniformity ru) {
		switch (ru) {
		case RegionUniformity::EMPTY: return RegionUniformity::FILLED;
		case RegionUniformity::FILLED: return RegionUniformity::EMPTY;
		case RegionUniformity::MIXED: return RegionUniformity::MIXED;
		default: abort(); return RegionUniformity::MIXED; // Note: should never happen, and actual return value irrelevant (won't get past abort()). Purpose: to make the compiler happy.
		}
	}

	enum struct Type : char {
		UNKNOWN = 0,
		II = 1,
		CII = 2,
		CBLQ_1D = 3,
		CBLQ_2D = 4,
		CBLQ_3D = 5,
		CBLQ_4D = 6,
		WAH = 7,
		UNCOMPRESSED_BITMAP = 8,
	};

	template<RegionEncoding::Type T> class TypeToClass { /*typedef XXX Class*/ };

	static boost::shared_ptr< RegionEncoding > make_null_region(RegionEncoding::Type type);
	static boost::shared_ptr< RegionEncoding > make_uniform_region(RegionEncoding::Type type, uint64_t nelem, bool filled);

	static boost::optional< std::type_index > get_region_representation_class_by_type(RegionEncoding::Type type);
	static boost::optional< Type > get_region_representation_type_by_name(std::string name);
	static boost::optional< std::string > get_region_representation_type_name(Type reptype);

	RegionEncoding(uint64_t domain_size = 0) : domain_size(domain_size) {}
	virtual ~RegionEncoding() {}

	virtual RegionEncoding::Type get_type() const = 0;
	virtual size_t get_size_in_bytes() const = 0;
	virtual void dump() const = 0;

	virtual RegionUniformity get_region_uniformity() const { return RegionUniformity::MIXED; }
	bool is_uniform() const { return this->get_region_uniformity() != RegionUniformity::MIXED; }

	virtual uint64_t get_domain_size() const { return this->domain_size; }

	virtual uint64_t get_element_count() const = 0;
	virtual void convert_to_rids(std::vector<uint32_t> &out, bool sorted = false, bool preserve_self = true) = 0;
	virtual void convert_to_rids(std::vector<uint64_t> &out, uint64_t offset, bool sorted = false, bool preserve_self = true);

	virtual void save_to_stream(std::ostream &out) = 0;
	virtual void load_from_stream(std::istream &in) = 0;

	virtual bool operator==(const RegionEncoding &other) const { return false; }
	bool operator!=(const RegionEncoding &other) const { return !(*this == other); }

protected:
	uint64_t domain_size;
};

// RegionEncoder, for producing RegionEncodings
template<typename RegionEncodingT, typename RegionEncoderConfigT>
class RegionEncoder {
public:
	typedef RegionEncodingT RegionEncodingOutT;
	typedef RegionEncoderConfigT RegionEncoderConfig;
public:
	RegionEncoder(RegionEncoderConfigT conf, size_t total_elements) :
		conf(conf),
		total_bit_count(total_elements),
		current_bit_count(0)
	{}
	virtual ~RegionEncoder() {}

	/**
	 * Pushes bits corresponding to the presence (bitval == true) or absence
	 * (bitval == false) of the next (count) elements within this region.
	 */
    void push_bits(uint64_t count, bool bitval);

    /**
     * Allows bits to be pushed at a location beyond the end of the last pushed bit.
     * Intervening bit positions preceding the new bits will be filled with 0s prior
     * to pushing the new bits. Cannot insert bits prior to the end of the last pushed
     * bit (i.e., a precondition is position >= immediately after the last pushed bit).
     */
    void insert_bits(uint64_t position, uint64_t count);

    /**
     * Pushes enough 0-bits to fill the rest of the index (up to total_elements
     * specified in the constructor).
     */
    void finalize();

    virtual boost::shared_ptr< RegionEncodingT > to_region_encoding() = 0;

protected:
    const RegionEncoderConfigT conf;

private:
    virtual void push_bits_impl(uint64_t count, bool bitval) = 0;
    virtual void finalize_impl() {};

    const uint64_t total_bit_count;
    uint64_t current_bit_count;
};

#include "impl/region-encoding-impl.hpp"

#endif /* REGION_ENCODING_HPP_ */
