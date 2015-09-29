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
 * cii-setops.cpp
 *
 *  Created on: Mar 22, 2014
 *      Author: David A. Boyuka II
 */

#include <vector>
#include <queue>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/region/cii/cii.hpp"
#include "pique/region/cii/cii-decoder.hpp"
#include "pique/setops/setops.hpp"
#include "pique/setops/cii/cii-setops.hpp"

#include "pique/util/list-setops.hpp"

//
// Actual set operations code
//

boost::shared_ptr< CIIRegionEncoding >
CIISetOperations::unary_set_op_impl(boost::shared_ptr< const CIIRegionEncoding > region, UnarySetOperation op) const {
	switch (op) {
	case UnarySetOperation::COMPLEMENT:
	{
		boost::shared_ptr< CIIRegionEncoding > output = boost::make_shared< CIIRegionEncoding >(*region);
		output->is_inverted = !output->is_inverted;
		return output;
	}
	default:
		abort();
	}

	return nullptr;
}


boost::shared_ptr< CIIRegionEncoding >
CIISetOperations::binary_set_op_impl(boost::shared_ptr< const CIIRegionEncoding > left, boost::shared_ptr< const CIIRegionEncoding > right, NArySetOperation op) const {
	if (left->is_compressed || right->is_compressed)
		return this->binary_set_op_mixed(left, right, op);
	else
		return this->binary_set_op_uncompressed(left, right, op);
}

boost::shared_ptr< CIIRegionEncoding >
CIISetOperations::binary_set_op_mixed(boost::shared_ptr< const CIIRegionEncoding > left, boost::shared_ptr< const CIIRegionEncoding > right, NArySetOperation op) const {
	assert(left->domain_size == right->domain_size);
	const rid_t domain_size = left->domain_size;

	ProgressiveCIIDecoder left_rids(*left);
	ProgressiveCIIDecoder right_rids(*right);
	boost::shared_ptr< CIIRegionEncoding > output = boost::make_shared< CIIRegionEncoding >();

	output->is_compressed = false;
	output->domain_size = domain_size;
	std::vector<rid_t> &output_rids = output->ii;

	while (left_rids.has_top() && right_rids.has_top()) {
		const rid_t left_rid = left_rids.top();
		const rid_t right_rid = right_rids.top();
		int rid_comp;
		if (left_rid < right_rid) {
			rid_comp = -1;
			left_rids.next();
		} else if (left_rid > right_rid) {
			rid_comp = 1;
			right_rids.next();
		} else {
			rid_comp = 0;
			left_rids.next();
			right_rids.next();
		}

		switch (op) {
		case NArySetOperation::UNION:
			if (rid_comp <= 0)
				output_rids.push_back(left_rid);
			else
				output_rids.push_back(right_rid);
			break;
		case NArySetOperation::INTERSECTION:
			if (rid_comp == 0)
				output_rids.push_back(left_rid);
			break;
		case NArySetOperation::DIFFERENCE:
			if (rid_comp == -1)
				output_rids.push_back(left_rid);
			break;
		case NArySetOperation::SYMMETRIC_DIFFERENCE:
			if (rid_comp == -1)
				output_rids.push_back(left_rid);
			else if (rid_comp == 1)
				output_rids.push_back(right_rid);
			break;
		}
	}

	// Append the remaining elements of the longer list, if the current operation dictates it
	if (left_rids.has_top() &&
			(op == NArySetOperation::UNION ||
			 op == NArySetOperation::DIFFERENCE ||
			 op == NArySetOperation::SYMMETRIC_DIFFERENCE)) {
		left_rids.dump_to(output_rids);
	}
	if (right_rids.has_top() &&
			(op == NArySetOperation::UNION ||
			 op == NArySetOperation::SYMMETRIC_DIFFERENCE)) {
		right_rids.dump_to(output_rids);
	}

	return output;
}

enum struct InvOperands { NEITHER, LEFT, RIGHT, BOTH };

// Takes two operands that may be inverted, and converts the given set operation to one
// that can be evaluated as if the operands were not inverted (i.e., pulls the
// complements outside the operands)
static void normalize_inverted_operands(
	bool left_inverted, bool right_inverted,
	boost::shared_ptr< const CIIRegionEncoding > &left, boost::shared_ptr< const CIIRegionEncoding > &right,
	NArySetOperation &op, bool &invert_result)
{
	const InvOperands invops =
		left_inverted ?
			(right_inverted ? InvOperands::BOTH : InvOperands::LEFT) :
			(right_inverted ? InvOperands::RIGHT : InvOperands::NEITHER);

	invert_result = false;
	if (invops != InvOperands::NEITHER) {
		switch (op) {
		// Union and intersection are very similar, complementary
		case NArySetOperation::UNION:
		case NArySetOperation::INTERSECTION:
			if (invops == InvOperands::BOTH) {
				// (!A) | (!B) == !(A & B)
				// (!A) & (!B) == !(A | B)
				op = (op == NArySetOperation::UNION) ? NArySetOperation::INTERSECTION : NArySetOperation::UNION;
				invert_result = true;
			} else { // LEFT or RIGHT
				// (!A) | B == !(A & !B) == !(A - B)
				// A | (!B) == !(!A & B) == !(B - A)
				// (!A) & B == B - A
				// A & (!B) == A - B
				if (op == NArySetOperation::INTERSECTION)	std::swap(left, right);
				if (invops == InvOperands::RIGHT)			std::swap(left, right);
				invert_result = (op == NArySetOperation::UNION);
				op = NArySetOperation::DIFFERENCE;
			}
			break;
		case NArySetOperation::DIFFERENCE:
			if (invops == InvOperands::BOTH) {
				// (!A) - (!B) == !A & !!B == B & !A = B - A
				std::swap(left, right);
			} else if (invops == InvOperands::LEFT) {
				// (!A) - B == !A & !B == !(A | B)
				op = NArySetOperation::UNION;
				invert_result = true;
			} else { // RIGHT
				// A - (!B) == A & !!B == A & B
				op = NArySetOperation::INTERSECTION;
			}
			break;
		case NArySetOperation::SYMMETRIC_DIFFERENCE:
			// (!A) xor (!B) == A xor B
			// (!A) xor B == !(A xor B)
			// A xor (!B) == !(A xor B)
			invert_result = (invops != InvOperands::BOTH);
		}
	}
}

boost::shared_ptr< CIIRegionEncoding >
CIISetOperations::binary_set_op_uncompressed(boost::shared_ptr< const CIIRegionEncoding > left, boost::shared_ptr< const CIIRegionEncoding > right, NArySetOperation op) const {
	assert(left->domain_size == right->domain_size);
	const rid_t domain_size = left->domain_size;

	bool invert_result;
	normalize_inverted_operands(left->is_inverted, right->is_inverted, left, right, op, invert_result);

	auto left_it = left->ii.cbegin();
	auto right_it = right->ii.cbegin();
	auto left_it_end = left->ii.cend();
	auto right_it_end = right->ii.cend();

	boost::shared_ptr< CIIRegionEncoding > output = boost::make_shared< CIIRegionEncoding >();
	output->domain_size = domain_size;
	output->is_compressed = false;
	output->is_inverted = invert_result;
	std::vector<rid_t> &output_rids = output->ii;

	list_set_operation<decltype(left_it), decltype(right_it), decltype(output_rids), rid_t>
		(op, left_it, left_it_end, right_it, right_it_end, output_rids);

	return output;
}

boost::shared_ptr< CIIRegionEncoding >
CIISetOperations::nary_set_op_impl(RegionEncodingCPtrCIter start_it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const {
	return this->nary_set_op_smartcompare_impl(start_it, end_it, op);
}

boost::shared_ptr< CIIRegionEncoding >
CIISetOperations::nary_set_op_smartcompare_impl(RegionEncodingCPtrCIter start_it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const {
	typedef boost::shared_ptr< const CIIRegionEncoding > const_RE_ptr;
	struct RECompareCompressedFirst {
		bool operator()(const const_RE_ptr& lhs, const const_RE_ptr&rhs) const {
			if (lhs->is_compressed != rhs->is_compressed)
				return !lhs->is_compressed; // lhs compressed <-> return false <-> lhs goes first
			else
				return lhs->get_size_in_bytes() > rhs->get_size_in_bytes();
		}
	};

	return this->nary_set_op_heapbased< RECompareCompressedFirst >(start_it, end_it, op);
}



boost::shared_ptr< CIIRegionEncoding > CIISetOperationsNAry::nary_set_op_impl(RegionEncodingCPtrCIter cii_it, RegionEncodingCPtrCIter cii_end_it, NArySetOperation op) const {
	if (op != NArySetOperation::UNION)
		return this->CIISetOperations::nary_set_op_smartcompare_impl(cii_it, cii_end_it, op);

	// Struct used for managing the front queue
	struct top_element {
		top_element(rid_t elem, size_t operand) :
			elem(elem), operand(operand)
		{}

		rid_t elem;
		size_t operand;
	};
	struct top_element_compare {
		// This is a max heap, so the comparison must be reversed
		bool operator() (const top_element& lhs, const top_element&rhs) const { return lhs.elem > rhs.elem; }
	};

	const rid_t domain_size = (*cii_it)->domain_size;

	// Capture the operand IIs
	size_t num_operands = 0;
	std::vector< ProgressiveCIIDecoder > rid_decoders;
	for (; cii_it != cii_end_it; cii_it++) {
		const CIIRegionEncoding &cii = **cii_it; // first * = deref iterator, second * = deref pointer to CII that the iterator was holding
		assert(cii.domain_size == domain_size);

		rid_decoders.emplace_back(cii);
		num_operands++;
	}

	// Create the output II
	boost::shared_ptr< CIIRegionEncoding > output = boost::make_shared< CIIRegionEncoding >();
	output->is_compressed = false;
	output->domain_size = domain_size;
	std::vector<rid_t> &output_rids = output->ii;

	// Set up the front priority queue
	std::priority_queue< top_element, std::vector<top_element>, top_element_compare > front_queue;
	for (size_t i = 0; i < num_operands; i++) {
		if (rid_decoders[i].has_top()) {
			const rid_t top_rid = rid_decoders[i].top();
			front_queue.emplace(top_rid, i);
			rid_decoders[i].next();
		}
	}

	// Alternately add the lowest RID (popped from the front of front_queue) to the output II and replace
	// it with the next-lowest RID from the operand from which it was originally taken
	rid_t last_rid = front_queue.top().elem + 1; // Any value that doesn't match the first element
	while (front_queue.size()) {
		// Pop the lowest RID element
		const top_element &next_elem = front_queue.top();
		const rid_t next_rid = next_elem.elem;
		const size_t fill_operand = next_elem.operand;
		ProgressiveCIIDecoder &fill_operand_decoder = rid_decoders[fill_operand];
		front_queue.pop();

		// Push it to the output if it is not a repeat RID
		if (next_rid != last_rid)
			output_rids.push_back(next_rid);

		// Record this RID so it is not repeated in the future
		last_rid = next_rid;

		// Replace it with the smallest remaining RID from the operand II
		// from which it was taken, if any remain
		if (fill_operand_decoder.has_top()) {
			front_queue.emplace(fill_operand_decoder.top(), fill_operand);
			fill_operand_decoder.next();
		}
	}

	return output;
}
