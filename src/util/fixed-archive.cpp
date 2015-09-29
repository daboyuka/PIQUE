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
 * fixed-archive.cpp
 *
 *  Created on: Sep 24, 2014
 *      Author: David A. Boyuka II
 */

#include "pique/util/fixed-archive.hpp"

#include <boost/archive/detail/archive_serializer_map.hpp>
#include <boost/archive/impl/archive_serializer_map.ipp>

namespace boost {
namespace archive {

// explicitly instantiate simple binary archive base classes and related templates
template class detail::archive_serializer_map<simple_binary_iarchive>;
template class simple_binary_iarchive_impl<simple_binary_iarchive, std::istream::char_type, std::istream::traits_type>;

template class detail::archive_serializer_map<simple_binary_oarchive>;
template class simple_binary_oarchive_impl<simple_binary_oarchive, std::ostream::char_type, std::ostream::traits_type>;

} // namespace archive
} // namespace boost
