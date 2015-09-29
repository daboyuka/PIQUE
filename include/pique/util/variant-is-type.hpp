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
 * variant-is-type.hpp
 *
 *  Created on: Jun 9, 2015
 *      Author: David A. Boyuka II
 */
#ifndef VARIANT_IS_TYPE_HPP_
#define VARIANT_IS_TYPE_HPP_

#include <boost/variant.hpp>

namespace detail {
template<typename CheckT>
struct ContainsTypeHelper {
	typedef bool result_type;

	template<typename T>
	bool operator()(const T &t) const { return false; }
	bool operator()(const CheckT &check_t) const { return true; }
};
}

template<typename CheckT, typename... VariantTypes>
bool variant_is_type(const boost::variant< VariantTypes... > &variant) {
	return boost::apply_visitor(detail::ContainsTypeHelper< CheckT >(), variant);
}

#endif /* VARIANT_IS_TYPE_HPP_ */
