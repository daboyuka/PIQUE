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
 * binned-index-types.hpp
 *
 *  Created on: Dec 9, 2014
 *      Author: David A. Boyuka II
 */
#ifndef BINNED_INDEX_TYPES_HPP_
#define BINNED_INDEX_TYPES_HPP_

#include <cstdint>

namespace BinnedIndexTypes {
	using bin_id_t = uint64_t;			// Position of a bin within the bin list
	using bin_count_t = bin_id_t;		// Number of bins in the bin list
	using region_id_t = bin_id_t;		// Position of a region within the region list
	using region_count_t = bin_count_t;	// Number of regions in the region list
	using bin_size_t = uint64_t;		// Number of elements in a bin
}

#endif /* BINNED_INDEX_TYPES_HPP_ */
