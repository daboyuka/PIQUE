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
 * wah-setops.hpp
 *
 *  Created on: Jul 11, 2014
 *      Author: David A. Boyuka II
 */
#pragma once

#include <boost/smart_ptr.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/wah/wah.hpp"
#include "pique/setops/setops.hpp"

struct WAHSetOperationsConfig {
	WAHSetOperationsConfig(bool compress_after_setops = false) :
		compress_after_setops(compress_after_setops)
	{}

	bool compress_after_setops;
};

class WAHSetOperations : public SetOperations< WAHRegionEncoding > {
public:
	typedef WAHSetOperationsConfig SetOperationsConfig;
	WAHSetOperations(WAHSetOperationsConfig conf) : conf(conf) {}
	virtual ~WAHSetOperations() {}

private:
    virtual boost::shared_ptr< WAHRegionEncoding > unary_set_op_impl(boost::shared_ptr< const WAHRegionEncoding > region, UnarySetOperation op) const;
    virtual boost::shared_ptr< WAHRegionEncoding > binary_set_op_impl(boost::shared_ptr< const WAHRegionEncoding > left, boost::shared_ptr< const WAHRegionEncoding > right, NArySetOperation op) const;

    using typename SetOperations< WAHRegionEncoding >::RegionEncodingCPtrCIter;
    virtual boost::shared_ptr< WAHRegionEncoding > nary_set_op_impl(RegionEncodingCPtrCIter region_it, RegionEncodingCPtrCIter region_end_it, NArySetOperation op) const;

protected:
    const WAHSetOperationsConfig conf;
};
