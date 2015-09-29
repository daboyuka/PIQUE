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
 * binarycomp-encoding.cpp
 *
 *  Created on: Dec 2, 2014
 *      Author: David A. Boyuka II
 */

#include "pique/encoding/binarycomp/binarycomp-encoding.hpp"

using bin_id_t = IndexEncoding::bin_id_t;
static void compute_binary_comp_region_math(const bin_id_t ub, const unsigned int num_bit_levels, RegionMath::RegionMath &rmath) {
	using region_id_t = IndexEncoding::region_id_t;

	bool region_pushed = false;
	for (region_id_t i = 0; i < num_bit_levels; ++i) {
		if (ub & (1ULL << i)) {
			rmath.push_region(i);
			if (region_pushed) {
				rmath.push_op(NArySetOperation::UNION, 2);
			}
			region_pushed = true;
		} else {
			if (region_pushed) {
				rmath.push_region(i).push_op(NArySetOperation::INTERSECTION, 2);
			}
		}
	}
}

auto BinaryComponentIndexEncoding::get_region_math_impl(bin_count_t nbins, bin_id_t lb, bin_id_t ub, bool prefer_complement) const -> RMath {
	// If we reach here, at least one and at most num_bins-1 bins are selected
	const bool has_subtractive_range = lb > 0;
	const bool has_additive_range = ub < nbins;

	unsigned int num_bit_levels = 0;
	bin_id_t binbits = nbins - 1;
	while (binbits) {
		binbits >>= 1;
		++num_bit_levels;
	}

	// Perform set operations on the additive/subtractive regions to obtain the result region
	RMath rmath;
	if (has_additive_range) {
		compute_binary_comp_region_math(ub, num_bit_levels, rmath);
		if (has_subtractive_range) {
			compute_binary_comp_region_math(lb, num_bit_levels, rmath);
			rmath.push_op(NArySetOperation::DIFFERENCE, 2);
		}
	} else {
		assert(has_subtractive_range); // Otherwise first == 0 and second == nbins, which is not allowed
		compute_binary_comp_region_math(lb, num_bit_levels, rmath);
		rmath.push_op(UnarySetOperation::COMPLEMENT);
	}
	return rmath;
}

auto BinaryComponentIndexEncoding::get_encoded_region_definitions_impl(bin_count_t nbins) const -> std::vector< bin_id_vector_t > {
    // Determine how many bits are needed to represent any region ID in the current index
    // Example: nregions = 8 -> nlayers = 3, nregions = 9 -> nlayers = 4, nregions = 10 -> nlayers = 4
    bin_count_t nlayers = 0;
    for (bin_count_t c = nbins - 1; c; c >>= 1)
    	++nlayers;

    std::vector< bin_id_vector_t > enc_region_defs;

    for (region_id_t layer = 0; layer < nlayers; layer++) {
    	const uint64_t binid_mask = (1ULL << layer);

    	bin_id_vector_t layer_region_def;
    	for (bin_id_t i = 0; i < nbins; i++)
    		if (!(i & binid_mask))
    			layer_region_def.push_back(i);

    	enc_region_defs.push_back(layer_region_def);
    }

    return enc_region_defs;
}
