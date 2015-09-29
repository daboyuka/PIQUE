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
 * interval-encoding.cpp
 *
 *  Created on: Dec 2, 2014
 *      Author: David A. Boyuka II
 */

#include <boost/iterator/counting_iterator.hpp>

#include "pique/encoding/interval/interval-encoding.hpp"

auto IntervalIndexEncoding::get_region_math_impl(bin_count_t nbins, bin_id_t lb, bin_id_t ub, bool prefer_complement) const -> RMath {
	using regid_t = region_id_t;

	const regid_t nregions = (nbins + 1) / 2; // ceil(nbins / 2)
	const regid_t interval_width = nbins / 2; // floor(nbins / 2)
	// Examples:
	// nbins == 7: [0, 3), [1, 4), [2, 5), [3, 6)   (interval_width = 3, nregions = 4)
	// nbins == 8: [0, 4), [1, 5), [2, 6), [3, 7)   (interval_width = 4, nregions = 4)

	// If the request interval covers the last bin, we have to use complement
	bool complement = false;
	if (ub == nbins) {
		ub = lb;
		lb = 0;
		complement = true;
	}

	// There are five cases, based (with modifications) on this seminal paper, page 6, eqn 6: http://dl.acm.org/citation.cfm?id=304201
	RMath rmath;
	if (ub < nregions) {
		// lb < ub < nregions from here
		rmath.push_region((regid_t)lb)
		     .push_region((regid_t)ub)
		     .push_op(NArySetOperation::DIFFERENCE);
	} else if (lb >= nregions) {
		// ub >= nregions >= interval_width from previous
		// lb >= nregions >= interval_width from here
		rmath.push_region((regid_t)ub - interval_width)
		     .push_region((regid_t)lb - interval_width)
		     .push_op(NArySetOperation::DIFFERENCE);
	} else if (ub - lb < interval_width) {
		// ub >= nregions >= interval_width from previous
		// lb < nregions from previous
		// ub - lb < interval_width from here
		rmath.push_region((regid_t)lb)
		     .push_region((regid_t)ub - interval_width)
		     .push_op(NArySetOperation::INTERSECTION);
	} else if (ub - lb > interval_width) {
		// ub >= nregions >= interval_width from previous
		// lb < nregions from previous
		// ub - lb >= interval_width from previous
		// ub - lb > interval_width from here
		rmath.push_region((regid_t)lb)
		     .push_region((regid_t)ub - interval_width)
		     .push_op(NArySetOperation::UNION);
	} else {
		// ub >= nregions >= interval_width from previous
		// lb < nregions from previous
		// ub - lb == interval_width from previous
		rmath.push_region((regid_t)lb);
	}

	// If we are computing the complement, invert the whole calculation at the end
	if (complement)
		rmath.push_op(UnarySetOperation::COMPLEMENT);

	return rmath;
}

auto IntervalIndexEncoding::get_encoded_region_definitions_impl(bin_count_t nbins) const -> std::vector< bin_id_vector_t > {
	const region_id_t nregions = (nbins + 1) / 2; // = ceil(nbins/2)
	const region_id_t interval_width = nbins / 2; //  = floor(nbins/2); intervals are of the form [x, x + interval_width)

	std::vector< bin_id_vector_t > enc_region_defs;

	for (region_id_t enc_region = 0; enc_region < nregions; ++enc_region) {
		bin_id_vector_t enc_region_def;
		// ith encoded region = [ith bin, (i+width) bin)
		enc_region_def.insert(
			enc_region_def.end(),
			boost::counting_iterator< bin_id_t >(enc_region),
			boost::counting_iterator< bin_id_t >(enc_region + interval_width));
		enc_region_defs.push_back(enc_region_def);
	}

	return enc_region_defs;
}

auto IntervalIndexEncoding::get_encoded_regions_impl(region_vector_t bins, const AbstractSetOperations& setops) const -> region_vector_t {
	region_vector_t enc_regions;
	const region_id_t nregions = (bins.size() + 1) / 2; // = ceil(nbins/2)
	const region_id_t interval_width = bins.size() / 2; //  = floor(nbins/2); intervals are of the form [x, x + interval_width)

	if (nregions > 0) {
		boost::shared_ptr< RegionEncoding > first_enc_region =
			setops.dynamic_nary_set_op(bins.begin(), bins.begin() + interval_width, NArySetOperation::UNION);
		enc_regions.push_back(first_enc_region);
	}

	for (region_id_t i = 1; i < nregions; ++i) {
		boost::shared_ptr< RegionEncoding > enc_region;

		// ith encoded region = (i-1)th encoded region - (i-1)th bin + (i-1+w)th bin
		enc_region = setops.dynamic_binary_set_op(enc_regions[i - 1], bins[i - 1], NArySetOperation::DIFFERENCE);
		enc_region = setops.dynamic_inplace_binary_set_op(enc_region, bins[i - 1 + interval_width], NArySetOperation::UNION);
		enc_regions.push_back(enc_region);
	}

	return enc_regions;
}
