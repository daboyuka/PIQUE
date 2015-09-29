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
 * index-io-types.hpp
 *
 *  Created on: Dec 9, 2014
 *      Author: David A. Boyuka II
 */
#ifndef INDEX_IO_TYPES_HPP_
#define INDEX_IO_TYPES_HPP_

#include <cstdint>
#include <utility> //std::pair

namespace IndexIOTypes {

using partition_id_t = uint64_t;
using partition_count_t = partition_id_t;
using domain_offset_t = uint64_t;
using domain_size_t = domain_offset_t;
using domain_t = std::pair< domain_offset_t, domain_size_t >;

using domain_id_t = partition_id_t; // Position in a list of sorted domains
using domain_mapping_t = std::pair< partition_id_t, domain_t >;

}

#endif /* INDEX_IO_TYPES_HPP_ */
