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
 * cblq-io.cpp
 *
 *  Created on: Apr 1, 2014
 *      Author: David A. Boyuka II
 */

#include <boost/smart_ptr.hpp>

#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-io.hpp"

template<int ndim>
size_t getRegionMetadataLength(boost::shared_ptr< const CBLQRegionEncoding<ndim> > region) {
	return
			sizeof(uint8_t) +			// For num levels
			sizeof(uint64_t) +			// Number of words
			sizeof(uint64_t) +			// Number of semiwords
			region->get_num_levels() *  // For each level...
				sizeof(uint64_t);		// ...store the level length
}

template<int ndim>
size_t getRegionDataLength(boost::shared_ptr< const CBLQRegionEncoding<ndim> > region) {
	return 	(region->get_num_words() * CBLQRegionEncoding<ndim>::BITS_PER_WORD + 7) / 8 +
			region->has_dense_suffix ?
					(region->dense_suffix->get_num_semiwords() * CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD + 7) / 8 :
					0;
}

template<int ndim>
void readRegionMetadata(boost::shared_ptr< CBLQRegionEncoding<ndim> > region, std::istream &out) {

}

template<int ndim>
void writeRegionMetadata(boost::shared_ptr< const CBLQRegionEncoding<ndim> > region, std::ostream &out) {

}

template<int ndim>
void readRegionData(boost::shared_ptr< CBLQRegionEncoding<ndim> > region, std::istream &out) {

}

template<int ndim>
void writeRegionData(boost::shared_ptr< const CBLQRegionEncoding<ndim> > region, std::ostream &out) {

}
