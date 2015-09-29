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
 * wah.cpp
 *
 *  Created on: Jul 11, 2014
 *      Author: David A. Boyuka II
 */

#include <iostream>
#include "pique/region/wah/wah.hpp"

constexpr RegionEncoding::Type WAHRegionEncoding::TYPE;

uint64_t WAHRegionEncoding::get_element_count() const {
	return this->bits.count();
}

void WAHRegionEncoding::dump() const {
	std::cout << "Domain size: " << this->bits.size() << std::endl;
	this->bits.print(std::cout);
}

void WAHRegionEncoding::convert_to_rids(std::vector<uint32_t>& out, bool sorted, bool preserve_self) {
	this->convert_to_rids< uint32_t, false >(out, 0);
}
void WAHRegionEncoding::convert_to_rids(std::vector<uint64_t>& out, uint64_t offset, bool sorted, bool preserve_self) {
	this->convert_to_rids< uint64_t, true >(out, offset);
}

template<typename rid_t, bool has_offset>
void WAHRegionEncoding::convert_to_rids(std::vector< rid_t > &out, uint64_t offset) {
	out.clear();
	out.reserve(this->bits.count());

	for (ibis::bitvector::indexSet is = this->bits.firstIndexSet(); is.nIndices() > 0; ++is) {
		const ibis::bitvector::word_t *ii = is.indices();
		if (is.isRange())
			for (ibis::bitvector::word_t j = *ii; j < ii[1]; ++ j)
				out.push_back(j + (has_offset ? offset : 0));
		else
			for (unsigned j = 0; j < is.nIndices(); ++ j)
				out.push_back(ii[j] + (has_offset ? offset : 0));
	}
}

bool WAHRegionEncoding::operator==(const RegionEncoding& other_base) const {
	if (typeid(other_base) != typeid(WAHRegionEncoding))
		return false;
	const WAHRegionEncoding &other = dynamic_cast<const WAHRegionEncoding&>(other_base);

	return this->bits == other.bits;
}
