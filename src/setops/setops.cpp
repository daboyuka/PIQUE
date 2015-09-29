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
 * setops.cpp
 *
 *  Created on: Aug 16, 2014
 *      Author: David A. Boyuka II
 */

#include <vector>
#include "pique/setops/setops.hpp"

using REPtr = boost::shared_ptr< RegionEncoding >;
using RECPtr = boost::shared_ptr< const RegionEncoding >;

using RU = RegionEncoding::RegionUniformity;

// SimplifiedSetOp

SimplifiedSetOp SimplifiedSetOp::append_operand(RU operand_ru) const {
	using NSO = NArySetOperation;
	using SSO = SimplifiedSetOp;

	const RU this_ru = this->result_uniformity;
	const NSO op = this->op;
	const bool compl_result = this->complement_op_result;

	switch (op) {
	case NSO::UNION:
		return	this_ru == RU::EMPTY ? SSO(operand_ru, op, compl_result) :
				this_ru == RU::FILLED ? SSO(RU::FILLED, op, compl_result) :
						// this_ru == RU::MIXED
						operand_ru == RU::EMPTY ? SSO(this_ru, op, compl_result) :
						operand_ru == RU::FILLED ? SSO(RU::FILLED, op, compl_result) :
							// operand_ru == RU::MIXED
							SSO(RU::MIXED, op, compl_result);
	case NSO::INTERSECTION:
		return	this_ru == RU::EMPTY ? SSO(RU::EMPTY, op, compl_result) :
				this_ru == RU::FILLED ? SSO(operand_ru, op, compl_result) :
						// this_ru == RU::MIXED
						operand_ru == RU::EMPTY ? SSO(RU::EMPTY, op, compl_result) :
						operand_ru == RU::FILLED ? SSO(this_ru, op, compl_result) :
							// operand_ru == RU::MIXED
							SSO(RU::MIXED, op, compl_result);
	case NSO::DIFFERENCE:
		return	this_ru == RU::EMPTY ? SSO(RU::EMPTY, op, compl_result) :
				this_ru == RU::FILLED ? SSO(operand_ru, NSO::UNION, !compl_result) :
						// this_ru == RU::MIXED
						operand_ru == RU::EMPTY ? SSO(this_ru, op, compl_result) :
						operand_ru == RU::FILLED ? SSO(RU::EMPTY, op, compl_result) :
							// operand_ru == RU::MIXED
							SSO(RU::MIXED, op, compl_result);
	case NSO::SYMMETRIC_DIFFERENCE:
		return	this_ru == RU::EMPTY ? SSO(operand_ru, op, compl_result) :
				this_ru == RU::FILLED ? SSO(operand_ru, op, !compl_result) :
						// this_ru == RU::MIXED
						operand_ru == RU::EMPTY ? SSO(this_ru, op, compl_result) :
						operand_ru == RU::FILLED ? SSO(this_ru, op, !compl_result) :
							// operand_ru == RU::MIXED
							SSO(RU::MIXED, op, compl_result);
	default:
		abort(); // Should never happen since all cases are handled above
		return SSO(this_ru, op, compl_result); // actual return irrelevant, just makes compiler happy
	}
}

SimplifiedSetOp SimplifiedSetOp::simplify(NArySetOperation op, const std::vector< RU > &operand_uniformities) {
	using SSO = SimplifiedSetOp;
	using NSO = NArySetOperation;

	// If zero operands, return identity
	if (operand_uniformities.empty())
	  return (op == NSO::INTERSECTION ? RU::FILLED : RU::EMPTY);

	auto op_it = operand_uniformities.cbegin();
	const auto op_end_it = operand_uniformities.cend();

	SimplifiedSetOp out(*op_it++, op, false);
	while (op_it != op_end_it)
		out = out.append_operand(*op_it++);

	out.finalize();
	return out;
}



auto AbstractSetOperations::dynamic_unary_set_op(RECPtr region, UnarySetOperation op) const -> REPtr {
	return this->dynamic_unary_set_op_impl(region, op);
}

auto AbstractSetOperations::dynamic_inplace_unary_set_op(REPtr region_and_out, UnarySetOperation op) const -> REPtr {
	this->dynamic_inplace_unary_set_op_impl(region_and_out, op);
	return region_and_out;
}

auto AbstractSetOperations::dynamic_binary_set_op(RECPtr left, RECPtr right, NArySetOperation op) const -> REPtr {
	return this->dynamic_binary_set_op_impl(left, right, op);
}

auto AbstractSetOperations::dynamic_inplace_binary_set_op(REPtr left_and_out, RECPtr right, NArySetOperation op) const -> REPtr {
	this->dynamic_inplace_binary_set_op_impl(left_and_out, right, op);
	return left_and_out;
}

// Non-const operands
// May return any operand as a result
REPtr AbstractSetOperations::dynamic_nary_set_op(RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const {
	const size_t operands = std::distance(it, end_it);
	if (operands == 0)
		return nullptr;
	else {
		REPtr first = *it;
		std::vector< RECPtr > constify_regions(++it, end_it);
		return this->dynamic_nary_set_op(first, constify_regions.begin(), constify_regions.end(), op);
	}
}

REPtr AbstractSetOperations::dynamic_inplace_nary_set_op(RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const {
	REPtr first_and_out = *it;
	return this->dynamic_inplace_nary_set_op(first_and_out, ++it, end_it, op);
}

REPtr AbstractSetOperations::dynamic_inplace_nary_set_op(REPtr first_and_out, RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const {
	std::vector< RECPtr > constify_regions(it, end_it);
	return this->dynamic_inplace_nary_set_op(first_and_out, constify_regions.begin(), constify_regions.end(), op);
}

// Const operands (except possibly the first)
// May return only a non-const operand as a result
RECPtr AbstractSetOperations::dynamic_nary_set_op(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const {
	const size_t operands = std::distance(it, end_it);
	if (operands == 0)
		return nullptr;
	else if (operands == 1)
		return *it;
	else
		return this->dynamic_nary_set_op_impl(it, end_it, op);
}

REPtr AbstractSetOperations::dynamic_nary_set_op(REPtr first, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const {
	const size_t other_operands = std::distance(it, end_it);
	if (other_operands == 0)
		return first;
	else {
		std::vector< RECPtr > all_operands;
		all_operands.push_back(first);
		all_operands.insert(all_operands.end(), it, end_it);
		return this->dynamic_nary_set_op_impl(all_operands.begin(), all_operands.end(), op);
	}
}

REPtr AbstractSetOperations::dynamic_inplace_nary_set_op(REPtr first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const {
	const size_t other_operands = std::distance(it, end_it);
	if (other_operands > 0)
		this->dynamic_inplace_nary_set_op_impl(first_and_out, it, end_it, op);

	return first_and_out;
}






// Nice idea, didn't work so well
/*
#include "pique/setops/cblq/impl/cblq-setops-actiontables.h"

static nary_action_t to_nary_action(NArySetOperation op, bool is_first_operand) {
	using NSO = NArySetOperation;
	switch (op) {
	case NSO::UNION:				return N_UNION;
	case NSO::INTERSECTION:			return N_INTER;
	case NSO::SYMMETRIC_DIFFERENCE: return N_SYMDIFF;
	case NSO::DIFFERENCE:			return N_DIFF;
	}
}

static std::pair< NArySetOperation, bool > to_nary_set_op(nary_action_t action) {
	using NSO = NArySetOperation;
	using OpComplPair = std::pair< NArySetOperation, bool >;
	switch (action) {
	case N_INFEAS:
	case N_DELETE:
		return OpComplPair(); // doesn't matter
	case N_UNION:		return OpComplPair(NSO::UNION, false);
	case N_INTER:		return OpComplPair(NSO::INTERSECTION, false);
	case N_DIFF:		return OpComplPair(NSO::DIFFERENCE, false);
	case N_CDIFF:		return OpComplPair(NSO::DIFFERENCE, true);
	case N_SYMDIFF:		return OpComplPair(NSO::SYMMETRIC_DIFFERENCE, false);
	case N_CSYMDIFF:	return OpComplPair(NSO::SYMMETRIC_DIFFERENCE, true);
	}
}

static int to_cblq_code(RU ru) { return (int)ru; } // we made sure this works
static RU to_region_uniformity(int cblq_code) { return (RU)cblq_code; } // we made sure this works

SimplifiedSetOp SimplifiedSetOp::append_operand(RU operand_ru) const {
	// Note: we notice that the result of appending an operand can be determined by a transition table.
	//       We then further notice that this transition table is identical to that of the N-ary CBLQ
	//       set ops transition table, since it is based on the same principle of empty/filled/mixed
	//       regions combined with N-ary set operations.
	//       Therefore, we translate the problem into CBLQ terms, use the transition table, and
	//       translate back.
	// TODO: Ideally, given this (belated) realization, the CBLQ transition table should instead be
	//       generalized and stored here, with a new CBLQ transition table being dervied from the
	//       general table. Oh well.

	const nary_action_t action = to_nary_action(this->op, is_first_operand);
	const int cur_code = to_cblq_code(this->result_uniformity);
	const int append_code = to_cblq_code(operand_uniformity);

	const auto &result = NARY_ACTION_TABLE[action][cur_code][append_code];

	const std::pair< NSO, bool > new_op_compl = to_nary_set_op(result.action);
	const NSO new_op = new_op_compl.first;
	const bool new_result_compl = new_op_compl.second;
	const RU new_result_uniformity = to_region_uniformity(result.output);

	return SimplifiedSetOp(new_result_uniformity, new_op, new_result_compl);
}
*/
