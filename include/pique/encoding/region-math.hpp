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
 * region-math.hpp
 *
 *  Created on: Dec 1, 2014
 *      Author: David A. Boyuka II
 */
#ifndef REGION_MATH_HPP_
#define REGION_MATH_HPP_

#include <vector>
#include <set>
#include <boost/variant.hpp>

#include "pique/indexing/binned-index-types.hpp"
#include "pique/region/region-encoding.hpp"
#include "pique/setops/setops.hpp"

namespace RegionMath {

using region_id_t = BinnedIndexTypes::region_id_t;

struct RegionTerm {
	RegionTerm(region_id_t rid) : rid(rid) {}
	const region_id_t rid;
};

struct UnaryOperatorTerm {
	UnaryOperatorTerm(UnarySetOperation op) : op(op) {}
	const UnarySetOperation op;
};

struct NAryOperatorTerm {
	NAryOperatorTerm(NArySetOperation op, int arity = 2) : op(op), arity(arity) {}
	const NArySetOperation op;
	const int arity;
};

using TermVariant = boost::variant< RegionTerm, UnaryOperatorTerm, NAryOperatorTerm >;

class RegionMath : public std::vector< TermVariant > {
public:
	static constexpr int REGION = 0;
	static constexpr int UNARY_OP = 1;
	static constexpr int NARY_OP = 2;

	RegionMath() {}
	RegionMath(region_id_t only) : std::vector< TermVariant >(1, RegionTerm(only)) {}

	RegionMath & push_region_range(region_id_t lb, region_id_t ub);
	RegionMath & push_region(region_id_t region_id);
	RegionMath & push_op(UnarySetOperation op);
	RegionMath & push_op(NArySetOperation op); // arity = all
	RegionMath & push_op(NArySetOperation op, int arity);
	RegionMath & push_region_op(region_id_t region_id, UnarySetOperation op); // push_region, push_op
	RegionMath & push_region_op(region_id_t region_id, NArySetOperation op); // push_region, push_op

	std::set< region_id_t > get_all_regions() const;

	std::string to_string();
};

}

#endif /* REGION_MATH_HPP_ */
