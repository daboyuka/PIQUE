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
 * wah-encode.cpp
 *
 *  Created on: Jul 11, 2014
 *      Author: David A. Boyuka II
 */

#include "pique/region/wah/wah-encode.hpp"

void WAHRegionEncoder::push_bits_impl(uint64_t count, bool bitval) {
	ibis::bitvector &bits = this->encoding->bits;
	bits.appendFill(bitval ? 1 : 0, count);
}

boost::shared_ptr< WAHRegionEncoding > WAHRegionEncoder::to_region_encoding() {
	return this->encoding;
}

