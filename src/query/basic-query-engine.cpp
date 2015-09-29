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
 * query-engine.cpp
 *
 *  Created on: May 14, 2014
 *      Author: David A. Boyuka II
 */

#include <string>
#include <iostream>
#include <cassert>
#include <iterator>
#include <set>

#include <boost/iterator/counting_iterator.hpp>

#include "pique/query/basic-query-engine.hpp"
#include "pique/util/timing.hpp"

boost::shared_ptr< RegionEncoding >
BasicQueryEngine::evaluate_set_op(boost::shared_ptr< RegionEncoding > region, bool is_region_mutable, UnarySetOperation op) const {
	return is_region_mutable ?
			setops->dynamic_inplace_unary_set_op(region, op) :
			setops->dynamic_unary_set_op(region, op);
}

boost::shared_ptr< RegionEncoding >
BasicQueryEngine::evaluate_set_op(RegionEncodingPtrCIter begin, RegionEncodingPtrCIter end, MutabilityCIter mutability_begin, MutabilityCIter mutability_end, NArySetOperation op) const {
	return *mutability_begin ?
			setops->dynamic_inplace_nary_set_op(begin, end, op) :
			setops->dynamic_nary_set_op(begin, end, op);
}

//
//void BasicQueryEngine::apply_unary_op_term(UnaryOperatorTerm opterm, std::vector< boost::shared_ptr< RegionEncoding > > &stack, MultivarTermEvalStats &terminfo) {
//	TIME_STATS_TIME_BEGIN(terminfo.total)
//	terminfo.name = 'a' + (int)opterm.op; // 'a', 'b', ...
//	terminfo.arity = 1;
//
//	boost::shared_ptr< RegionEncoding > region = stack.back();
//	stack.pop_back();
//
//	terminfo.in_regions_bytes = region->get_size_in_bytes();
//
//	boost::shared_ptr< RegionEncoding > new_region = this->setops->dynamic_unary_set_op(region, opterm.op);
//	stack.push_back(new_region);
//	TIME_STATS_TIME_END()
//}
//
//void BasicQueryEngine::apply_nary_op_term(NAryOperatorTerm opterm, std::vector< boost::shared_ptr< RegionEncoding > > &stack, MultivarTermEvalStats &terminfo) {
//	TIME_STATS_TIME_BEGIN(terminfo.total)
//	terminfo.name = 'A' + (int)opterm.op; // 'A', 'B', ...
//	terminfo.arity = opterm.arity;
//
//	auto begin_it = stack.end() - opterm.arity;
//	auto end_it = stack.end();
//
//	// Count total bytes being processed
//	terminfo.in_regions_bytes = 0;
//	for (auto it = begin_it; it != end_it; ++it)
//		terminfo.in_regions_bytes += (*it)->get_size_in_bytes();
//
//	boost::shared_ptr< RegionEncoding > new_region = this->setops->dynamic_nary_set_op(begin_it, end_it, opterm.op);
//	stack.erase(begin_it, end_it);
//	stack.push_back(new_region);
//	TIME_STATS_TIME_END()
//}
