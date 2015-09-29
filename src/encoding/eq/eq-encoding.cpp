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
 * eq-encoding.cpp
 *
 *  Created on: Dec 2, 2014
 *      Author: David A. Boyuka II
 */

#include "pique/encoding/eq/eq-encoding.hpp"

auto EqualityIndexEncoding::get_region_math_impl(bin_count_t nbins, bin_id_t lb, bin_id_t ub, bool prefer_complement) const -> RMath {
	using regid_t = region_id_t;
	if (prefer_complement) {
		return RMath()
				.push_region_range(0, (regid_t)lb)
				.push_region_range((regid_t)ub, (regid_t)nbins)
				.push_op(NArySetOperation::UNION)
				.push_op(UnarySetOperation::COMPLEMENT);
	} else {
		return RMath()
				.push_region_range((regid_t)lb, (regid_t)ub)
				.push_op(NArySetOperation::UNION);
	}
}

auto EqualityIndexEncoding::get_encoded_region_definitions_impl(bin_count_t nbins) const -> std::vector< bin_id_vector_t > {
	std::vector< bin_id_vector_t > enc_region_defs;
	for (bin_id_t i = 0; i < nbins; ++i)
		enc_region_defs.push_back(bin_id_vector_t(1, i)); // The ith encoded region = the ith bin region

	return enc_region_defs;
}

auto EqualityIndexEncoding::get_encoded_regions_impl(region_vector_t bins, const AbstractSetOperations& setops) const -> region_vector_t {
	return bins; // No transformation needed
}
