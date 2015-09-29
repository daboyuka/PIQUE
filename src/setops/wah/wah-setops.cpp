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
 * wah-setops.cpp
 *
 *  Created on: Jul 11, 2014
 *      Author: David A. Boyuka II
 */

#include <boost/make_shared.hpp>
#include "pique/setops/wah/wah-setops.hpp"

boost::shared_ptr< WAHRegionEncoding > WAHSetOperations::unary_set_op_impl(boost::shared_ptr< const WAHRegionEncoding > region, UnarySetOperation op) const {
	assert(op == UnarySetOperation::COMPLEMENT);

	boost::shared_ptr< WAHRegionEncoding > output = boost::make_shared< WAHRegionEncoding >();

	output->bits = region->bits;
	output->bits.flip();

	// No need for compression before returning, as bit flipping never results in a more compressible bitvector
	return output;
}

boost::shared_ptr< WAHRegionEncoding > WAHSetOperations::binary_set_op_impl(boost::shared_ptr< const WAHRegionEncoding > left, boost::shared_ptr< const WAHRegionEncoding > right, NArySetOperation op) const {
	// Prepare a WAHRegionEncoding to return
	boost::shared_ptr< WAHRegionEncoding > output = boost::make_shared< WAHRegionEncoding >();

	// Compute the new bitvector
	std::unique_ptr< ibis::bitvector > newbits;
	switch (op) {
	case NArySetOperation::UNION:
		newbits = std::unique_ptr< ibis::bitvector >(left->bits | right->bits);
		break;
	case NArySetOperation::INTERSECTION:
		newbits = std::unique_ptr< ibis::bitvector >(left->bits & right->bits);
		break;
	case NArySetOperation::DIFFERENCE:
		newbits = std::unique_ptr< ibis::bitvector >(left->bits - right->bits);
		break;
	case NArySetOperation::SYMMETRIC_DIFFERENCE:
		newbits = std::unique_ptr< ibis::bitvector >(left->bits ^ right->bits);
		break;
	}

	// Swap the new bitvector contents into the WAHRegionEncoding and return
	output->bits.swap(*newbits);

	if (this->conf.compress_after_setops)
		output->bits.compress();

	return output;
}

boost::shared_ptr< WAHRegionEncoding > WAHSetOperations::nary_set_op_impl(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const {
	boost::shared_ptr< WAHRegionEncoding > output = this->nary_set_op_default_impl(it, end_it, op);

	if (this->conf.compress_after_setops)
		output->bits.compress();

	return output;
}
