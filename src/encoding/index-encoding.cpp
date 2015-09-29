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
 * index-encoding.cpp
 *
 *  Created on: Dec 2, 2014
 *      Author: David A. Boyuka II
 */

#include <iostream>
#include <boost/smart_ptr.hpp>

#include "pique/encoding/index-encoding.hpp"
#include "pique/util/dynamic-dispatch.hpp"

#include "pique/indexing/binned-index.hpp"
#include "pique/setops/setops.hpp"

#include "pique/encoding/eq/eq-encoding.hpp"
#include "pique/encoding/range/range-encoding.hpp"
#include "pique/encoding/interval/interval-encoding.hpp"
#include "pique/encoding/binarycomp/binarycomp-encoding.hpp"
#include "pique/encoding/hier/hier-encoding.hpp"

using EncType = IndexEncoding::Type;

typedef typename
	MakeValueToTypeDispatch< EncType >
	::WithValues< EncType::EQUALITY, EncType::RANGE, EncType::INTERVAL, EncType::BINARY_COMPONENT, EncType::HIERARCHICAL >
	::WithTypes< EqualityIndexEncoding, RangeIndexEncoding, IntervalIndexEncoding, BinaryComponentIndexEncoding, HierarchicalIndexEncoding >
	::type EncTypeDispatch;

boost::shared_ptr< IndexEncoding > IndexEncoding::get_instance(Type type) {
	return EncTypeDispatch::template dispatchMatching< boost::shared_ptr< IndexEncoding > >(
			type,
			boost::make_shared_dispatch< IndexEncoding >(),
			nullptr);
}

auto IndexEncoding::get_encoded_index(
	boost::shared_ptr< const IndexEncoding > index_enc,
	boost::shared_ptr< const BinnedIndex > in_index,
	const AbstractSetOperations &setops
	) -> boost::shared_ptr< BinnedIndex >
{
	// Assume the input index is equality-encoded, so that encoded regions == bin regions
	assert(in_index->get_encoding()->get_type() == Type::EQUALITY);

	region_vector_t bin_regions;
	in_index->get_regions(bin_regions);

	region_vector_t encoded_regions = index_enc->get_encoded_regions(bin_regions, setops);
	return in_index->derive_new_index(index_enc, encoded_regions);
}

auto IndexEncoding::get_encoded_regions_impl(region_vector_t bins, const AbstractSetOperations& setops) const -> region_vector_t {
	const std::vector< bin_id_vector_t > enc_region_defs = this->get_encoded_region_definitions(bins.size());

	region_vector_t enc_regions;
	for (const bin_id_vector_t &enc_region_def : enc_region_defs) {
		region_vector_t bins_to_merge;
		for (const bin_id_t bin : enc_region_def)
			bins_to_merge.push_back(bins[bin]);

		boost::shared_ptr< RegionEncoding > enc_region =
			setops.dynamic_nary_set_op(bins_to_merge.begin(), bins_to_merge.end(), NArySetOperation::UNION);

		enc_regions.push_back(enc_region);
	}

	return enc_regions;
}
