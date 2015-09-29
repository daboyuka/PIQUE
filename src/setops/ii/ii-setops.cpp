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
 * ii-setops.cpp
 *
 *  Created on: Mar 20, 2014
 *      Author: David A. Boyuka II
 */

#include <vector>

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/region/ii/ii.hpp"
#include "pique/setops/setops.hpp"
#include "pique/setops/ii/ii-setops.hpp"
#include "pique/util/list-setops.hpp"

boost::shared_ptr< IIRegionEncoding >
IISetOperations::unary_set_op_impl(
		boost::shared_ptr< const IIRegionEncoding > region,
		UnarySetOperation op) const
{
	boost::shared_ptr< IIRegionEncoding > output = boost::make_shared< IIRegionEncoding >();
	output->domain_size = region->domain_size;

	const std::vector<rid_t> &input_rids = region->rids;
	std::vector<rid_t> &output_rids = output->rids;

	switch (op) {
	case UnarySetOperation::COMPLEMENT:
	{
		rid_t next_absent_rid = 0;
		for (auto rid_it = input_rids.cbegin(); rid_it != input_rids.cend(); ++rid_it) {
			const rid_t next_present_rid = *rid_it;
			for (rid_t i = next_absent_rid; i < next_present_rid; i++)
				output_rids.push_back(i);

			next_absent_rid = next_present_rid + 1;
		}

		for (rid_t i = next_absent_rid; i < region->domain_size; i++)
			output_rids.push_back(i);
		break;
	}
	default:
		abort();
	}

	return output;
}

boost::shared_ptr< IIRegionEncoding >
IISetOperations::binary_set_op_impl(
		boost::shared_ptr< const IIRegionEncoding > left,
		boost::shared_ptr< const IIRegionEncoding > right,
		NArySetOperation op) const
{
	assert(left->domain_size == right->domain_size);
	const rid_t domain_size = left->domain_size;

	auto left_it = left->rids.cbegin();
	auto right_it = right->rids.cbegin();
	auto left_it_end = left->rids.cend();
	auto right_it_end = right->rids.cend();

	boost::shared_ptr< IIRegionEncoding > output = boost::make_shared< IIRegionEncoding >(domain_size);
	std::vector<rid_t> &output_rids = output->rids;

	list_set_operation<decltype(left_it), decltype(right_it), decltype(output_rids), rid_t>
		(op, left_it, left_it_end, right_it, right_it_end, output_rids);

	return output;
}

boost::shared_ptr< IIRegionEncoding >
IISetOperationsNAry::nary_set_op_impl(
		RegionEncodingCPtrCIter region_it,
		RegionEncodingCPtrCIter region_end_it,
		NArySetOperation op) const
{
	// Only N-ary union implemented for now
	if (op != NArySetOperation::UNION)
		abort();

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

	const rid_t domain_size = (*region_it)->domain_size;

	// Capture the operand IIs
	size_t num_operands = 0;
	std::vector< typename std::vector<rid_t>::const_iterator > rid_its;
	std::vector< typename std::vector<rid_t>::const_iterator > rid_end_its;
	for (; region_it != region_end_it; region_it++) {
		const IIRegionEncoding &ii = **region_it; // first * = deref iterator, second * = deref pointer to II that the iterator was holding
		assert(ii.domain_size == domain_size);

		num_operands++;
		rid_its.push_back(ii.rids.cbegin());
		rid_end_its.push_back(ii.rids.cend());
	}

	// Create the output II
	boost::shared_ptr< IIRegionEncoding > output = boost::make_shared< IIRegionEncoding >(domain_size);
	std::vector<rid_t> &output_rids = output->rids;

	// Set up the front priority queue
	std::priority_queue< top_element, std::vector<top_element>, top_element_compare > front_queue;
	for (size_t i = 0; i < num_operands; i++)
		if (rid_its[i] != rid_end_its[i])
			front_queue.emplace(*rid_its[i]++, i);

	// Alternately add the lowest RID (popped from the front of front_queue) to the output II and replace
	// it with the next-lowest RID from the operand from which it was originally taken
	rid_t last_rid = front_queue.top().elem + 1; // Any value that doesn't match the first element
	while (front_queue.size()) {
		// Pop the lowest RID element
		const top_element &next_elem = front_queue.top();
		const rid_t next_rid = next_elem.elem;
		const size_t fill_operand = next_elem.operand;
		front_queue.pop();

		// Push it to the output if it is not a repeat RID
		if (next_rid != last_rid)
			output_rids.push_back(next_rid);

		// Record this RID so it is not repeated in the future
		last_rid = next_rid;

		// Replace it with the smallest remaining RID from the operand II
		// from which it was taken, if any remain
		if (rid_its[fill_operand] != rid_end_its[fill_operand])
			front_queue.emplace(*rid_its[fill_operand]++, fill_operand);
	}

	return output;
}
