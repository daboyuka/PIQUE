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
 * preflist-setops.cpp
 *
 *  Created on: Jun 9, 2015
 *      Author: David A. Boyuka II
 */
#ifndef SRC_SETOPS_IMPL_PREFLIST_SETOPS_CPP_
#define SRC_SETOPS_IMPL_PREFLIST_SETOPS_CPP_

#include "pique/setops/setops.hpp"

// Attempts to apply any of a list of AbstractSetOperations to a given set of operands; the first AbstractSetOperations that
// accepts all operand types is applied.
bool PreferenceListSetOperations::can_handle_region_encoding(RegionEncoding::Type type) const {
	return std::any_of(
			this->begin(), this->end(),
			[type](boost::shared_ptr< const AbstractSetOperations > setops) -> bool {
		return setops->can_handle_region_encoding(type);
	});
}

auto PreferenceListSetOperations::dynamic_unary_set_op_impl(RECPtr region, UnarySetOperation op) const -> REPtr {
	const RegionEncoding::Type type = region->get_type();

	for (auto it = this->begin(); it != this->end(); it++)
		if ((*it)->can_handle_region_encoding(type))
			return (*it)->dynamic_unary_set_op(region, op);

	abort(); // Unsupported region encoding type
	return nullptr;
}

void PreferenceListSetOperations::dynamic_inplace_unary_set_op_impl(REPtr region_and_out, UnarySetOperation op) const {
	const RegionEncoding::Type type = region_and_out->get_type();

	for (auto it = this->begin(); it != this->end(); it++) {
		if ((*it)->can_handle_region_encoding(type)) {
			(*it)->dynamic_inplace_unary_set_op(region_and_out, op);
			return;
		}
	}

	abort(); // Unsupported region encoding type
}

auto PreferenceListSetOperations::dynamic_binary_set_op_impl(RECPtr left, RECPtr right, NArySetOperation op) const -> REPtr {
	const RegionEncoding::Type ltype = left->get_type();
	const RegionEncoding::Type rtype = right->get_type();
	for (auto it = this->begin(); it != this->end(); it++)
		if ((*it)->can_handle_region_encoding(ltype) &&
				(*it)->can_handle_region_encoding(rtype))
			return (*it)->dynamic_binary_set_op(left, right, op);

	abort(); // Unsupported region encoding type
	return nullptr;
}

void PreferenceListSetOperations::dynamic_inplace_binary_set_op_impl(REPtr left_and_out, RECPtr right, NArySetOperation op) const {
	const RegionEncoding::Type ltype = left_and_out->get_type();
	const RegionEncoding::Type rtype = right->get_type();
	for (auto it = this->begin(); it != this->end(); it++) {
		if ((*it)->can_handle_region_encoding(ltype) &&
				(*it)->can_handle_region_encoding(rtype)) {
			return;
		}
	}

	abort(); // Unsupported region encoding type
}

auto PreferenceListSetOperations::dynamic_nary_set_op_impl(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const -> REPtr {
	for (auto setopsit = this->begin(); setopsit != this->end(); setopsit++) {
		boost::shared_ptr< AbstractSetOperations > setops = *setopsit;
		bool success = true;
		for (auto operit = it; operit != end_it; operit++) {
			if (!setops->can_handle_region_encoding((*operit)->get_type())) {
				success = false;
				break;
			}
		}

		if (success)
			return setops->dynamic_nary_set_op_impl(it, end_it, op);
	}

	abort(); // Unsupported region encoding type
	return nullptr;
}

void PreferenceListSetOperations::dynamic_inplace_nary_set_op_impl(REPtr first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const {
	for (auto setopsit = this->begin(); setopsit != this->end(); setopsit++) {
		boost::shared_ptr< AbstractSetOperations > setops = *setopsit;
		bool success = true;
		for (auto operit = it; operit != end_it; operit++) {
			if (!setops->can_handle_region_encoding((*operit)->get_type())) {
				success = false;
				break;
			}
		}

		if (success)
			return;
	}

	abort(); // Unsupported region encoding type
}




#endif /* SRC_SETOPS_IMPL_PREFLIST_SETOPS_CPP_ */
