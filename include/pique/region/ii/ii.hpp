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
 * ii.hpp
 *
 *  Created on: Feb 7, 2014
 *      Author: David A. Boyuka II
 */

#pragma once

#include <iterator>
#include <cstdint>
#include <boost/smart_ptr.hpp>
#include <boost/limits.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/serialization/vector.hpp>

#include "pique/region/region-encoding.hpp"

#include "pique/util/fixed-archive.hpp"

// IIRegionEncoding, a concrete subclass of RegionEncoding utilizing a II encoding
// TODO: Use infinite tail encoding (bool tail_is_present) to efficiently handle filled uniform regions
class IIRegionEncoding : public RegionEncoding {
public:
	static constexpr RegionEncoding::Type TYPE = RegionEncoding::Type::II;
	typedef uint32_t rid_t;

public:
	IIRegionEncoding() : domain_size(0) {}
	IIRegionEncoding(uint64_t domain_size) : domain_size(domain_size) {}
	IIRegionEncoding(uint64_t nelem, bool filled) :
		domain_size(static_cast<rid_t>(nelem)), rids(boost::counting_iterator<rid_t>(0), boost::counting_iterator<rid_t>(filled ? static_cast<rid_t>(nelem) : 0))
	{
		assert(nelem <= std::numeric_limits<rid_t>::max());
	}
	template<typename IterT> IIRegionEncoding(IterT begin, IterT end, uint64_t domain_size) : domain_size(domain_size), rids(begin, end) {}
	IIRegionEncoding(IIRegionEncoding &&) noexcept = default; // Explicitly allow move construction
	virtual ~IIRegionEncoding() {}

	IIRegionEncoding & operator=(IIRegionEncoding &&) = default; // Explicitly allow move assignment

	virtual RegionEncoding::Type get_type() const { return RegionEncoding::Type::II; }

    virtual size_t get_size_in_bytes() const { return rids.size() * sizeof(rid_t); }
    virtual void dump() const;

    virtual uint64_t get_domain_size() const { return domain_size; }
	virtual uint64_t get_element_count() const { return rids.size(); }
	virtual void convert_to_rids(std::vector<uint32_t> &out, bool sorted = false, bool preserve_self = true) {
		if (preserve_self)
			out = rids;
		else
			out = std::move(rids);
	}

    size_t get_num_rids() const { return rids.size(); }

	virtual void save_to_stream(std::ostream &out) { boost::archive::simple_binary_oarchive(out, boost::archive::no_header) << *this; }
	virtual void load_from_stream(std::istream &in) { boost::archive::simple_binary_iarchive(in, boost::archive::no_header) >> *this; }

	virtual bool operator==(const RegionEncoding &other) const;
private:
    friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	ar & domain_size;
    	ar & rids;
    }

private:
    rid_t domain_size;
    std::vector<rid_t> rids;

    friend class IIRegionEncoder;
    friend class IISetOperations;
    friend class IISetOperationsNAry;
};

template<> class RegionEncoding::TypeToClass<RegionEncoding::Type::II> { typedef IIRegionEncoding REClass; };

BOOST_CLASS_IMPLEMENTATION(IIRegionEncoding, boost::serialization::object_serializable) // no version information serialized
