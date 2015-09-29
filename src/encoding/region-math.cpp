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
 * region-math.cpp
 *
 *  Created on: Dec 2, 2014
 *      Author: David A. Boyuka II
 */

#include <string>
#include <vector>
#include <map>
#include <algorithm>

#include "pique/encoding/region-math.hpp"

namespace RegionMath {

RegionMath & RegionMath::push_region(region_id_t region_id) {
	this->push_back(TermVariant(RegionTerm(region_id)));
	return *this;
}

RegionMath & RegionMath::push_region_range(region_id_t lb, region_id_t ub) {
	for (region_id_t i = lb; i < ub; ++i)
		this->push_region(i);
	return *this;
}

RegionMath & RegionMath::push_op(UnarySetOperation op) {
	this->push_back(TermVariant(UnaryOperatorTerm(op)));
	return *this;
}

RegionMath & RegionMath::push_op(NArySetOperation op) {
	return this->push_op(op, this->size());
}

RegionMath & RegionMath::push_op(NArySetOperation op, int arity) {
	this->push_back(TermVariant(NAryOperatorTerm(op, arity)));
	return *this;
}

RegionMath & RegionMath::push_region_op(region_id_t region_id, UnarySetOperation op) {
	return this->push_region(region_id).push_op(op);
}

RegionMath & RegionMath::push_region_op(region_id_t region_id, NArySetOperation op) {
	return this->push_region(region_id).push_op(op);
}

auto RegionMath::get_all_regions() const -> std::set< region_id_t > {
	struct collect_region_ids : public boost::static_visitor<> {
		std::set< region_id_t > region_ids;

		void operator()(const RegionTerm region_term) { region_ids.insert(region_term.rid); }
		void operator()(const UnaryOperatorTerm &) {}
		void operator()(const NAryOperatorTerm &) {}
	};

	collect_region_ids collector;
	std::for_each(this->cbegin(), this->cend(), boost::apply_visitor(collector));
	return collector.region_ids;
}

}

std::string RegionMath::RegionMath::to_string() {
	static const std::map< NArySetOperation, std::string > OP_SYMS = {
			{NArySetOperation::UNION, "|"},
			{NArySetOperation::INTERSECTION, "&"},
			{NArySetOperation::DIFFERENCE, "-"},
			{NArySetOperation::SYMMETRIC_DIFFERENCE, "^"},
	};
	struct StringizeVisitor {
		typedef void result_type;
		StringizeVisitor() {}
		void operator()(const RegionTerm &region) {
			stack.push_back(std::to_string(region.rid));
		}
		void operator()(const UnaryOperatorTerm &op) {
			stack.back() = "-" + stack.back();
		}
		void operator()(const NAryOperatorTerm &op) {
			auto operands_begin = stack.end() - op.arity;
			auto it = operands_begin + 1;
			while (it != stack.end())
				*operands_begin += " " + OP_SYMS.at(op.op) + " " + *it++;
			*operands_begin = "(" + *operands_begin + ")";
			stack.erase(operands_begin + 1, stack.end());
		}

		std::vector< std::string > stack;
	};

	StringizeVisitor visitor;
	for (const TermVariant &term : *this)
		boost::apply_visitor(visitor, term);

	return visitor.stack.back();
}
