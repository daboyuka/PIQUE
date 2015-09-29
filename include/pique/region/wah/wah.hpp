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
 * wah.hpp
 *
 *  Created on: Jul 10, 2014
 *      Author: David A. Boyuka II
 */
#ifndef WAH_HPP_
#define WAH_HPP_

#include <iterator>
#include <cstdint>
#include <boost/smart_ptr.hpp>
#include <boost/limits.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/binary_object.hpp>

// FastBit library
#include <bitvector.h>

#include "pique/region/region-encoding.hpp"

#include "pique/util/fixed-archive.hpp"

// WAHRegionEncoding, a concrete subclass of RegionEncoding utilizing a WAH-compressed bitmap encoding
class WAHRegionEncoding : public RegionEncoding {
public:
	static constexpr RegionEncoding::Type TYPE = RegionEncoding::Type::WAH;

public:
	WAHRegionEncoding() : bits() {}
	WAHRegionEncoding(uint64_t domain_size) : bits() {
		assert(domain_size <= std::numeric_limits< ibis::bitvector::word_t >::max());
		bits.appendFill(0, domain_size);
	}
	WAHRegionEncoding(uint64_t nelem, bool filled) : bits() {
		assert(nelem <= std::numeric_limits< ibis::bitvector::word_t >::max());
		bits.appendFill(filled ? 1 : 0, (ibis::bitvector::word_t)nelem);
	}
	virtual ~WAHRegionEncoding() {}

	virtual RegionEncoding::Type get_type() const { return RegionEncoding::Type::WAH; }

    virtual size_t get_size_in_bytes() const { return bits.getSerialSize(); }
    virtual void dump() const;

    virtual uint64_t get_domain_size() const { return bits.size(); }
	virtual uint64_t get_element_count() const;
	virtual void convert_to_rids(std::vector<uint32_t> &out, bool sorted = false, bool preserve_self = true);
	virtual void convert_to_rids(std::vector<uint64_t> &out, uint64_t offset, bool sorted = false, bool preserve_self = true);

	virtual void save_to_stream(std::ostream &out) { boost::archive::simple_binary_oarchive(out, boost::archive::no_header) << *this; }
	virtual void load_from_stream(std::istream &in) { boost::archive::simple_binary_iarchive(in, boost::archive::no_header) >> *this; }

	virtual bool operator==(const RegionEncoding &other) const;

private:
	template<typename rid_t, bool has_offset>
	void convert_to_rids(std::vector< rid_t > &out, uint64_t offset);

    friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	using word_t = ibis::bitvector::word_t;

    	if (Archive::is_loading::value)
    		bits = ibis::bitvector();

    	ar & bits.nbits;
    	ar & bits.nset;
    	ar & bits.active.nbits;
    	ar & bits.active.val;

		size_t n;
		if (Archive::is_saving::value)
			n = bits.m_vec.size();
		ar & n;
		if (Archive::is_loading::value)
    		bits.m_vec = ibis::array_t< word_t >(n);

		ar & boost::serialization::make_binary_object(
				(void*)bits.m_vec.begin(),
				n * sizeof(word_t));
    }

private:
    ibis::bitvector bits;

    friend class WAHRegionEncoder;
    friend class WAHSetOperations;
    friend class WAHToBitmapConverter;
};

template<> class RegionEncoding::TypeToClass<RegionEncoding::Type::WAH> { typedef WAHRegionEncoding REClass; };

BOOST_CLASS_IMPLEMENTATION(WAHRegionEncoding, boost::serialization::object_serializable) // no version information serialized

#endif /* WAH_HPP_ */
