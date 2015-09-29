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
 * wah-encode.hpp
 *
 *  Created on: Jul 11, 2014
 *      Author: David A. Boyuka II
 */

#pragma once

#include <cassert>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/wah/wah.hpp"

struct WAHRegionEncoderConfig {};

class WAHRegionEncoder : public RegionEncoder< WAHRegionEncoding, WAHRegionEncoderConfig > {
public:
	WAHRegionEncoder(WAHRegionEncoderConfig conf, size_t total_elements) :
		RegionEncoder< WAHRegionEncoding, WAHRegionEncoderConfig >(conf, total_elements)
	{
		encoding = boost::make_shared< WAHRegionEncoding >();
	}
    virtual ~WAHRegionEncoder() {}

    virtual boost::shared_ptr< WAHRegionEncoding > to_region_encoding();

private:
    virtual void push_bits_impl(uint64_t count, bool bitval);

    boost::shared_ptr< WAHRegionEncoding > encoding;
};
