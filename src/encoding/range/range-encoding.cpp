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
 * range-encoding.cpp
 *
 *  Created on: Dec 2, 2014
 *      Author: David A. Boyuka II
 */

#include "pique/encoding/range/range-encoding.hpp"

auto RangeIndexEncoding::get_region_math_impl(bin_count_t nbins, bin_id_t lb, bin_id_t ub, bool prefer_complement) const -> RMath {
	using regid_t = region_id_t;

	// Ignore prefer_complement, as complement is always a bad idea: it's superset-or-equal to the non-complement region math
	RMath rmath;
	if (ub < nbins) {
		assert(ub > 0); // Should always be true
		rmath.push_region((regid_t)ub - 1);
		if (lb > 0)
			rmath.push_region_op((regid_t)lb - 1, NArySetOperation::DIFFERENCE);
	} else {
		assert(lb > 0); // Otherwise first == 0 and second == nbins, which is not allowed
		rmath.push_region_op((regid_t)lb - 1, UnarySetOperation::COMPLEMENT);
	}
	return rmath;
}

auto RangeIndexEncoding::get_encoded_region_definitions_impl(bin_count_t nbins) const -> std::vector< bin_id_vector_t > {
	std::vector< bin_id_vector_t > enc_region_defs;
	for (bin_id_t i = 0; i < nbins - 1; ++i) {
		// Start with the previous encoded region definition, if it exists
		bin_id_vector_t ith_region = (i > 0 ? enc_region_defs[i-1] : bin_id_vector_t());
		// Add the current bin
		ith_region.push_back(i);
		enc_region_defs.push_back(ith_region);
	}
	return enc_region_defs;
}

auto RangeIndexEncoding::get_encoded_regions_impl(region_vector_t bins, const AbstractSetOperations& setops) const -> region_vector_t {
    region_vector_t encoded_regions;
    encoded_regions.reserve(bins.size() - 1);

    for (bin_id_t i = 0; i < bins.size() - 1; i++) {
    	boost::shared_ptr< RegionEncoding > encoded_region;

    	if (i == 0)
    		encoded_region = bins[0];
    	else
    		encoded_region = setops.dynamic_binary_set_op(encoded_regions[i-1], bins[i], NArySetOperation::UNION);

    	encoded_regions.push_back(encoded_region);
    }

    return encoded_regions;
}
