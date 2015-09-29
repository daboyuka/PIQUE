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
 * hier-encoding.cpp
 *
 *  Created on: Dec 2, 2014
 *      Author: David A. Boyuka II
 */

#include <set>
#include <boost/iterator/counting_iterator.hpp>

#include "pique/encoding/hier/hier-encoding.hpp"

// exclusive upper bound
using bin_id_t = IndexEncoding::bin_id_t;
static void build_hierarchical_range(bin_id_t bin_ub, std::set< bin_id_t > &bins) {
	bin_ub = (bin_ub - 1 + 1); // bin_ub must become inclusive (-1), and must also become 1-based (+1)

	bin_id_t bitmask = 1;
	while (bin_ub) {
		if (bin_ub & bitmask) {
			bins.insert(bin_ub - 1 /* convert back to 0-based */);
			bin_ub -= bitmask;
		}
		bitmask <<= 1;
	}
}

template<typename ElemT>
static void inplace_symmetric_difference(std::set< ElemT > &s1, std::set< ElemT > &s2) {
	std::set< ElemT > s12;
	std::set_intersection(s1.begin(), s1.end(), s2.begin(), s2.end(), std::inserter(s12, s12.end()));
	for (const ElemT &e : s12) {
		s1.erase(e);
		s2.erase(e);
	}
}

auto HierarchicalIndexEncoding::get_region_math_impl(bin_count_t nbins, bin_id_t lb, bin_id_t ub, bool prefer_complement) const -> RMath {
	using regid_t = region_id_t;

	// If we reach here, at least one and at most num_bins-1 bins are selected
	const bool has_subtractive_range = lb > 0;
	const bool has_additive_range = ub < nbins;

	// Set up to read both the additive and subtractive bins
	std::set< region_id_t > additive_regions, subtractive_regions;
	if (has_additive_range)
		build_hierarchical_range(ub, additive_regions);
	if (has_subtractive_range)
		build_hierarchical_range(lb, subtractive_regions);

	// If we have both additive and subtractive bins, don't bother reading or
	// processing those bins that appear as both
	if (has_additive_range && has_subtractive_range)
		inplace_symmetric_difference(additive_regions, subtractive_regions);

	RMath rmath;
	if (has_additive_range) {
		for (bin_id_t ar : additive_regions) rmath.push_region((regid_t)ar);
		rmath.push_op(NArySetOperation::UNION);

		if (has_subtractive_range) {
			for (bin_id_t sr : subtractive_regions) rmath.push_region((regid_t)sr);
			rmath.push_op(NArySetOperation::DIFFERENCE, 1 + subtractive_regions.size());
		}
	} else {
		assert(has_subtractive_range); // Otherwise first == 0 and second == nbins, which is not allowed
		for (bin_id_t sr : subtractive_regions) rmath.push_region((regid_t)sr);
		rmath.push_op(NArySetOperation::UNION)
		     .push_op(UnarySetOperation::COMPLEMENT);
	}
	return rmath;
}


auto HierarchicalIndexEncoding::get_encoded_region_definitions_impl(bin_count_t nbins) const -> std::vector< bin_id_vector_t > {
	const region_count_t nregions = nbins - 1;

	std::vector< bin_id_vector_t > enc_region_defs;

	for (region_id_t i = 0; i < nregions; i ++) {
		const region_id_t bins_to_merge_end = i + 1;

		// Find the least-sig. 1-bit in bins_to_merge_end
		region_count_t bins_to_merge_len;
		for (bins_to_merge_len = 1; (bins_to_merge_end & bins_to_merge_len) == 0; bins_to_merge_len <<= 1);

		const region_id_t bins_to_merge_begin = bins_to_merge_end - bins_to_merge_len;

		bin_id_vector_t enc_region_def;
		enc_region_def.insert(
			enc_region_def.end(),
			boost::counting_iterator< bin_id_t >(bins_to_merge_begin),
			boost::counting_iterator< bin_id_t >(bins_to_merge_end));

		enc_region_defs.push_back(std::move(enc_region_def));

	}

	return enc_region_defs;
}

auto HierarchicalIndexEncoding::get_encoded_regions_impl(region_vector_t bins, const AbstractSetOperations& setops) const -> region_vector_t {
	const region_count_t nregions = bins.size() - 1;

	region_vector_t enc_regions;

	for (region_id_t i = 0; i < nregions; i ++) {
		region_vector_t enc_regions_to_merge(1, bins[i]); // Always include the ith bin to merge into this encoded region

		// For every trailing 1 in the binary representation of the current region ID, we need
		// to merge with another region (the ID of which is computed by masking out one of these 1s)
		// Example: region ID 10111 needs to be merged with 10110, 10101 and 10011 (all numbers in binary)
		region_id_t trailing_one_mask = 1;
		while (i & trailing_one_mask) {
			region_id_t merge_region_id = i & ~trailing_one_mask;
			trailing_one_mask <<= 1;

			enc_regions_to_merge.push_back(enc_regions[merge_region_id]); // Get the region from the NEW index (we need merged bins)
		}

		boost::shared_ptr< RegionEncoding > enc_region =
			setops.dynamic_nary_set_op(
				enc_regions_to_merge.begin(),
				enc_regions_to_merge.end(),
				NArySetOperation::UNION
			);

		enc_regions.push_back(enc_region);
	}

	return enc_regions;
}
