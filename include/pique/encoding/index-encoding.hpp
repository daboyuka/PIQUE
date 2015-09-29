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
 * index-encoding.hpp
 *
 * Note: all subclasses X must have a public default constructor.
 *
 *  Created on: Dec 1, 2014
 *      Author: David A. Boyuka II
 */
#ifndef INDEX_ENCODING_HPP_
#define INDEX_ENCODING_HPP_

#include <iostream>
#include <vector>
#include <boost/smart_ptr.hpp>

#include "pique/indexing/binned-index-types.hpp"
class BinnedIndex;
class AbstractSetOperations;
#include "pique/encoding/region-math.hpp"

class IndexEncoding {
public:
	using bin_id_t = BinnedIndexTypes::bin_id_t;
	using bin_count_t = BinnedIndexTypes::bin_count_t;
	using region_id_t = BinnedIndexTypes::region_id_t;
	using region_count_t = BinnedIndexTypes::region_count_t;
	using RMath = RegionMath::RegionMath;

	using bin_id_vector_t = std::vector< bin_id_t >;
	using region_vector_t = std::vector< boost::shared_ptr< RegionEncoding > >;

	enum struct Type : char { EQUALITY, RANGE, INTERVAL, HIERARCHICAL, BINARY_COMPONENT };

public:
	// This works only for those index encodings that take no parameters
	static boost::shared_ptr< IndexEncoding > get_instance(Type type);
	static boost::shared_ptr< IndexEncoding > get_equality_encoding_instance() { return get_instance(Type::EQUALITY); }

	static boost::shared_ptr< BinnedIndex > get_encoded_index(
			boost::shared_ptr< const IndexEncoding > index_enc,
			boost::shared_ptr< const BinnedIndex > in_index,
			const AbstractSetOperations &setops);

protected:
	IndexEncoding(Type type) : type(type) {}

public:
	virtual ~IndexEncoding() {}

	Type get_type() const { return type; }

	virtual bool operator==(const IndexEncoding &other) const { return this->get_type() == other.get_type(); }

	// Indexing side

	/*
	 * Returns a vector of bin ID sets. The ith bin ID set specifies
	 * the bins composing the ith encoded region.
	 *
	 * Rough example:
	 *   auto X = get_encoded_region_definitions(nbins)
	 *   encoded_region[i] = setops.nary_set_op(X[i].begin(), X[i].end(), NArySetOperation::UNION);
	 *
	 * This function isn't used as often as get_encoded_regions, for
	 * efficiency reasons; however, successfully implementing this
	 * function proves that an IndexEncoding subclass is valid.
	 */
	std::vector< bin_id_vector_t > get_encoded_region_definitions(bin_count_t nbins) const {
		return this->get_encoded_region_definitions_impl(nbins);
	}

	/*
	 * Similar to get_encoded_region_definitions, but actually computes the encoded regions.
	 * This may be more efficient than calling get_encoded_region_definitions and following
	 * its returned instructions, as this function can reuse computations across multiple
	 * encoded region in some cases.
	 */
	region_vector_t get_encoded_regions(region_vector_t bins, const AbstractSetOperations &setops) const {
		return this->get_encoded_regions_impl(bins, setops);
	}

	// Query side
	RMath get_region_math(bin_count_t nbins, bin_id_t lb, bin_id_t ub, bool prefer_complement = false) const { // [lb, ub)
		return this->get_region_math_impl(nbins, lb, ub, prefer_complement);
	}

private:
	// Delegates
	virtual std::vector< bin_id_vector_t > get_encoded_region_definitions_impl(bin_count_t nbins) const = 0;
	virtual region_vector_t get_encoded_regions_impl(region_vector_t bins, const AbstractSetOperations &setops) const; // Default: use get_encoded_region_definitions_impl and simple unions

	virtual RMath get_region_math_impl(bin_count_t nbins, bin_id_t lb, bin_id_t ub, bool prefer_complement) const = 0; // [lb, ub)

	virtual void load_from_stream_impl(std::istream &in) {} // Default: no extra information to deserialize
	virtual void save_to_stream_impl(std::ostream &out)  {} // Default: no extra information to deserialize

private:
	const Type type;
};

#endif /* INDEX_ENCODING_HPP_ */



