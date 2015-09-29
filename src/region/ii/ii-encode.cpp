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
 * ii-encode.cpp
 *
 *  Created on: Feb 7, 2014
 *      Author: David A. Boyuka II
 */

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/region/ii/ii-encode.hpp"

IIRegionEncoder::IIRegionEncoder(IIRegionEncoderConfig conf, size_t total_elements) :
	RegionEncoder< IIRegionEncoding, IIRegionEncoderConfig >(conf, total_elements),
	next_rid(0),
	encoding(boost::make_shared< IIRegionEncoding >(total_elements))
{}

boost::shared_ptr< IIRegionEncoding > IIRegionEncoder::to_region_encoding() {
	return this->encoding;
}

void IIRegionEncoder::push_bits_impl(uint64_t count, bool bitval) {
	// Only actually push 1-bits as RIDs; do nothing for 0-bits
	if (bitval)
		for (rid_t rid = next_rid; rid < next_rid + count; rid++)
			this->encoding->rids.push_back(rid);

	// Advance the current RID (bit) position in any case (this is the only
	// effect for 0-bit pushes)
	next_rid += count;
}
