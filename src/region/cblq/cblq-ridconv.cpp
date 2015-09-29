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
 * cblq-ridconv.cpp
 *
 *  Created on: Jul 9, 2014
 *      Author: David A. Boyuka II
 */

#include <cstdint>
#include <vector>
#include <boost/iterator/counting_iterator.hpp>

#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-traversal.hpp"

template<int ndim>
template<typename rid_t, bool has_offset>
void CBLQRegionEncoding<ndim>::convert_to_rids_sorted(std::vector<rid_t> &out, uint64_t offset, bool preserve_self) {
	using block_size_t = typename CBLQTraversalBlocks<ndim>::block_size_t;
	using block_offset_t = typename CBLQTraversalBlocks<ndim>::block_offset_t;

	const uint64_t domain_size = this->domain_size;
	out.clear();

	const uint64_t real_offset = (has_offset ? offset : 0); // Compiles out if has_offset == false

	auto process_block_fn = [domain_size, &out, real_offset](block_size_t block_size, block_offset_t block_offset) -> void {
		const uint64_t block_end_offset = std::min(block_offset + block_size, domain_size);
		if (block_end_offset > block_offset) {
			out.insert(
					out.end(),
					boost::counting_iterator< rid_t >(static_cast< rid_t >(block_offset + real_offset)),
					boost::counting_iterator< rid_t >(static_cast< rid_t >(block_end_offset + real_offset))
			);
		}
	};

	auto process_leaf_block_fn = [domain_size, &out, real_offset](block_size_t /*block_size == 1, unused*/, block_offset_t block_offset) -> void {
		if (block_offset < domain_size)
			out.push_back(block_offset + real_offset);
	};

	CBLQTraversalBlocks<ndim>::template traverse_df_leafopt<true>(*this, process_block_fn, process_leaf_block_fn);
}


template<int ndim>
void CBLQRegionEncoding<ndim>::convert_to_rids(std::vector<uint32_t> &out, bool sorted, bool preserve_self) {
	return this->convert_to_rids_sorted< uint32_t, false >(out, 0, preserve_self);
}

template<int ndim>
void CBLQRegionEncoding<ndim>::convert_to_rids(std::vector<uint64_t> &out, uint64_t offset, bool sorted, bool preserve_self) {
	return this->convert_to_rids_sorted< uint64_t, true >(out, offset, preserve_self);
}


// Explicit instantiation of 2D-4D CBLQ region encodings
// (this is also done in cblq.cpp, but multiple instantiations are OK; this forces the functions declared in this file to be instantiated)
template class CBLQRegionEncoding<2>;
template class CBLQRegionEncoding<3>;
template class CBLQRegionEncoding<4>;
