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
 * setops-impl.hpp
 *
 *  Created on: Mar 19, 2014
 *      Author: David A. Boyuka II
 */
#ifndef SETOPS_IMPL_HPP_
#define SETOPS_IMPL_HPP_

#include <vector>
#include <queue>
#include <boost/smart_ptr.hpp>

//
// Misc. functions
//
template<typename RE>
bool SetOperations< RE >::can_handle_region_encoding(RegionEncoding::Type type) const {
	boost::optional< std::type_index > check_ti = RegionEncoding::get_region_representation_class_by_type(type);
	return check_ti && *check_ti == typeid(RegionEncodingType); // Can only handle regions of the specific encoding declared by template parameter
}



//
// New API
//

// Unary
template<typename RE>
auto SetOperations< RE >::unary_set_op(RECPtrT region, UnarySetOperation op) const -> REPtrT {
	return this->unary_set_op_impl(region, op);
}

template<typename RE>
auto SetOperations< RE >::inplace_unary_set_op(REPtrT region_and_out, UnarySetOperation op) const ->REPtrT {
	this->inplace_unary_set_op_impl(region_and_out, op);
	return region_and_out;
}

// Binary
template<typename RE>
auto SetOperations< RE >::binary_set_op(RECPtrT left, RECPtrT right, NArySetOperation op) const -> REPtrT {
	return this->binary_set_op_impl(left, right, op);
}

template<typename RE>
auto SetOperations< RE >::inplace_binary_set_op(REPtrT left_and_out, RECPtrT right, NArySetOperation op) const -> REPtrT {
	this->inplace_binary_set_op_impl(left_and_out, right, op);
	return left_and_out;
}

// N-ary (non-const operands)
template<typename RE>
auto SetOperations< RE >::nary_set_op(RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const -> REPtrT {
	const size_t operands = std::distance(it, end_it);
	if (operands == 0) {
		return nullptr;
	} else {
		REPtrT first = *it;
		std::vector< RECPtrT > constify_regions(++it, end_it);
		return this->nary_set_op(first, constify_regions.cbegin(), constify_regions.cend(), op);
	}
}

template<typename RE>
auto SetOperations< RE >::inplace_nary_set_op(RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const -> REPtrT {
	boost::shared_ptr< RE > first_and_out = *it;
	return this->inplace_nary_set_op(first_and_out, ++it, end_it, op);
}

template<typename RE>
auto SetOperations< RE >::inplace_nary_set_op(REPtrT first_and_out, RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const -> REPtrT {
	std::vector< RECPtrT > constify_regions(it, end_it);
	return this->inplace_nary_set_op(first_and_out, constify_regions.cbegin(), constify_regions.cend(), op);
}

// N-ary (const operands, except possibly the first)
template<typename RE>
auto SetOperations< RE >::nary_set_op(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const -> RECPtrT {
	const size_t operands = std::distance(it, end_it);
	if (operands == 0)
		return nullptr;
	else if (operands == 1)
		return *it;
	else
		return this->nary_set_op_impl(it, end_it, op);
}

template<typename RE>
auto SetOperations< RE >::nary_set_op(REPtrT first, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const -> REPtrT {
	const size_t other_operands = std::distance(it, end_it);
	if (other_operands > 0) {
		std::vector< RECPtrT > all_operands;
		all_operands.push_back(first);
		all_operands.insert(all_operands.end(), it, end_it);
		return this->nary_set_op_impl(all_operands.cbegin(), all_operands.cend(), op);
	} else {
		return first;
	}
}

template<typename RE>
auto SetOperations< RE >::inplace_nary_set_op(REPtrT first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const -> REPtrT {
	const size_t other_operands = std::distance(it, end_it);
	if (other_operands > 0)
		this->inplace_nary_set_op_impl(first_and_out, it, end_it, op);

	return first_and_out;
}



//
// AbstractSetOperations impl. stub overrides
//
template<typename RE>
auto SetOperations< RE >::dynamic_unary_set_op_impl(RECPtr region, UnarySetOperation op) const -> REPtr {
	if (region->get_type() != RegionEncodingType::TYPE) abort(); //return nullptr;
	return this->unary_set_op(
			boost::dynamic_pointer_cast< const RE >(region),
			op);
}

template<typename RE>
void SetOperations< RE >::dynamic_inplace_unary_set_op_impl(REPtr region_and_out, UnarySetOperation op) const {
	if (region_and_out->get_type() != RegionEncodingType::TYPE) abort(); //return;
	this->inplace_unary_set_op(
		boost::dynamic_pointer_cast< RE >(region_and_out),
		op);
}

template<typename RE>
auto SetOperations< RE >::dynamic_binary_set_op_impl(RECPtr left, RECPtr right, NArySetOperation op) const -> REPtr {
	if (left->get_type() != RegionEncodingType::TYPE ||
		right->get_type() != RegionEncodingType::TYPE)
		abort(); //return nullptr;
	return this->binary_set_op(
			boost::dynamic_pointer_cast< const RE >(left),
			boost::dynamic_pointer_cast< const RE >(right),
			op);
}

template<typename RE>
void SetOperations< RE >::dynamic_inplace_binary_set_op_impl(REPtr left_and_out, RECPtr right, NArySetOperation op) const {
	if (left_and_out->get_type() != RegionEncodingType::TYPE ||
		right->get_type() != RegionEncodingType::TYPE)
		abort(); //return;
	this->inplace_binary_set_op(
			boost::dynamic_pointer_cast< RE >(left_and_out),
			boost::dynamic_pointer_cast< const RE >(right),
			op);
}

template<typename RE>
auto SetOperations< RE >::dynamic_nary_set_op_impl(GRegionEncodingCPtrCIter it, GRegionEncodingCPtrCIter end_it, NArySetOperation op) const -> REPtr {
	std::vector< boost::shared_ptr< const RE > > downcast_regions;
	downcast_regions.reserve(end_it - it);

	for (auto downcast_it = it; downcast_it != end_it; downcast_it++) {
		if ((*downcast_it)->get_type() != RegionEncodingType::TYPE) abort(); //return nullptr;
		downcast_regions.push_back(boost::dynamic_pointer_cast< const RE, const RegionEncoding >(*downcast_it));
	}

	return this->nary_set_op_impl(
			downcast_regions.cbegin(),
			downcast_regions.cend(),
			op);
}

template<typename RE>
void SetOperations< RE >::dynamic_inplace_nary_set_op_impl(REPtr first_and_out, GRegionEncodingCPtrCIter it, GRegionEncodingCPtrCIter end_it, NArySetOperation op) const {
	if (first_and_out->get_type() != RegionEncodingType::TYPE) abort(); //return;
	boost::shared_ptr< RE > downcast_first_and_out = boost::dynamic_pointer_cast< RE, RegionEncoding >(first_and_out);

	std::vector< boost::shared_ptr< const RE > > downcast_regions;
	downcast_regions.reserve(end_it - it);

	for (auto downcast_it = it; downcast_it != end_it; downcast_it++) {
		if ((*downcast_it)->get_type() != RegionEncodingType::TYPE) abort(); //return;
		downcast_regions.push_back(boost::dynamic_pointer_cast< const RE, const RegionEncoding >(*downcast_it));
	}

	this->inplace_nary_set_op(
			downcast_first_and_out,
			downcast_regions.cbegin(),
			downcast_regions.cend(),
			op);
}



//
// Default impl. stub implementations
//

template<typename RE>
void SetOperations< RE >::inplace_unary_set_op_impl(REPtrT region_and_out, UnarySetOperation op) const {
	REPtrT out = this->unary_set_op(region_and_out, op);

	if (!out) abort(); //return;
	*region_and_out = std::move(*out);
}

template<typename RE>
void SetOperations< RE >::inplace_binary_set_op_impl(REPtrT left_and_out, RECPtrT right, NArySetOperation op) const {
	REPtrT out = this->binary_set_op(left_and_out, right, op);

	if (!out) abort(); //return;
	*left_and_out = std::move(*out);
}

template<typename RE>
auto SetOperations< RE >::nary_set_op_impl(RegionEncodingCPtrCIter start_it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const -> REPtrT {
	return this->nary_set_op_default_impl(start_it, end_it, op);
}

template<typename RE>
void SetOperations< RE >::inplace_nary_set_op_impl(REPtrT first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const {
	REPtrT out = this->nary_set_op(first_and_out, it, end_it, op);

	if (!out) abort(); //return;
	*first_and_out = std::move(*out);
}



//
// Helper functions for this class and subclasses
//

template<typename RE>
auto SetOperations< RE >::nary_set_op_default_impl(RegionEncodingCPtrCIter start_it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const -> REPtrT {
	return is_set_op_symmetric(op) ?
			this->nary_set_op_heapbased< RECompareReverseSizeOrder >(start_it, end_it, op) :
			this->nary_set_op_direct(start_it, end_it, op);
}

// Balanced pairwise implementation, using priority queue-based set balancing
// Note: heap-based operation balancing inspired by FastBit's method for doing this
template<typename RE>
template<typename RECompare>
auto SetOperations< RE >::nary_set_op_heapbased(RegionEncodingCPtrCIter start_it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const -> REPtrT {
	std::priority_queue< RECPtrT, std::vector< RECPtrT >, RECompare > regions(start_it, end_it);
	assert(start_it != end_it && ++start_it != end_it); // Assert at least two elements

	while (regions.size() > 1) {
		RECPtrT left_re = regions.top();
		regions.pop();
		RECPtrT right_re = regions.top();
		regions.pop();

		REPtrT reduced = this->binary_set_op(left_re, right_re, op);
		if (regions.empty())
			return reduced; // Return immediately, because once we put the region back in the heap, it's no longer non-const

		regions.push(reduced);
	}

	abort();
	return nullptr;
}

template<typename RE>
auto SetOperations< RE >::nary_set_op_direct(RegionEncodingCPtrCIter start_it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const -> REPtrT {
	assert(start_it != end_it); // Assert at least one element
	RECPtrT first = *start_it++;
	assert(start_it != end_it); // Assert at least two elements
	RECPtrT second = *start_it++;

	REPtrT result = this->binary_set_op(first, second, op);

	for (; start_it != end_it; ++start_it)
		result = this->binary_set_op(result, *start_it, op);

	return result;
}

template<typename SOB>
auto GroupedNArySetOperations<SOB>::nary_set_op_impl(RegionEncodingCPtrCIter start_it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const ->
	boost::shared_ptr< typename SetOperationsBasis::RegionEncodingType >
{
	typedef typename SetOperationsBasis::RegionEncoding RE;
	typedef boost::shared_ptr< RE > RE_ptr;
	typedef boost::shared_ptr< const RE > const_RE_ptr;

	std::priority_queue< const_RE_ptr, std::vector< const_RE_ptr >, typename SetOperations< RegionEncodingBasis >::RECompareReverseSizeOrder > regions(start_it, end_it);
	std::vector< const_RE_ptr > group_regions;
	group_regions.reserve(this->group_size);

	while (regions.size() > 1) {
		for (int i = 0; i < this->group_size && !regions.empty(); i++) {
			const_RE_ptr re = regions.top();
			regions.pop();
			group_regions.push_back(re);
		}

		RE_ptr reduced = this->SetOperationsBasis::nary_set_op(group_regions.cbegin(), group_regions.cend(), op);
		if (regions.empty())
			return reduced; // Return immediately, because once we put the region back in the heap, it's no longer non-const

		regions.push(reduced);
		group_regions.clear();
	}

	abort();
	return nullptr;
}

template<typename SOB>
void GroupedNArySetOperations<SOB>::inplace_nary_set_op_impl(
		boost::shared_ptr< RegionEncodingBasis > first_and_out,
		RegionEncodingCPtrCIter start_it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const
{
	typedef typename SetOperationsBasis::RegionEncoding RE;
	typedef boost::shared_ptr< const RE > const_RE_ptr;

	std::priority_queue< const_RE_ptr, std::vector< const_RE_ptr >, typename SetOperations< RegionEncodingBasis >::RECompareReverseSizeOrder > other_regions(start_it, end_it);
	std::vector< const_RE_ptr > group_regions;
	group_regions.reserve(this->group_size);

	while (other_regions.size() > 0) {
		for (int i = 0; i < (this->group_size - 1) && !other_regions.empty(); i++) {
			const_RE_ptr re = other_regions.top();
			other_regions.pop();
			group_regions.push_back(re);
		}

		this->SetOperationsBasis::nary_inplace_set_op(first_and_out, group_regions.cbegin(), group_regions.cend(), op);
		group_regions.clear();
	}
}

#endif /* SETOPS_IMPL_HPP_ */
