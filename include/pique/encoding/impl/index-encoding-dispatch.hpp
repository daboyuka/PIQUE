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
 * index-encoding-dispatch.hpp
 *
 * Utility file that defines the dynamic dispatch template for encoding types.
 *
 *  Created on: Aug 26, 2015
 *      Author: David A. Boyuka II
 */
#ifndef SRC_ENCODING_IMPL_INDEX_ENCODING_DISPATCH_HPP_
#define SRC_ENCODING_IMPL_INDEX_ENCODING_DISPATCH_HPP_

#include "pique/util/dynamic-dispatch.hpp"
#include "pique/encoding/index-encoding.hpp"

using EncType = IndexEncoding::Type;

class EqualityIndexEncoding;
class RangeIndexEncoding;
class IntervalIndexEncoding;
class BinaryComponentIndexEncoding;
class HierarchicalIndexEncoding;

typedef typename
	MakeValueToTypeDispatch< EncType >
	::WithValues< EncType::EQUALITY, EncType::RANGE, EncType::INTERVAL, EncType::BINARY_COMPONENT, EncType::HIERARCHICAL >
	::WithTypes< EqualityIndexEncoding, RangeIndexEncoding, IntervalIndexEncoding, BinaryComponentIndexEncoding, HierarchicalIndexEncoding >
	::type EncTypeDispatch;

#endif /* SRC_ENCODING_IMPL_INDEX_ENCODING_DISPATCH_HPP_ */
