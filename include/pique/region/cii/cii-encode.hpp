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
 * cii-encode.hpp
 *
 *  Created on: Feb 28, 2014
 *      Author: David A. Boyuka II
 */

#pragma once

#include <cassert>
#include <vector>
#include <deque>
#include <boost/shared_ptr.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/cii/cii.hpp"

#include <patchedframeofreference.h>

struct CIIRegionEncoderConfig {};

class CIIRegionEncoder : public RegionEncoder< CIIRegionEncoding, CIIRegionEncoderConfig > {
public:
	typedef CIIRegionEncoding::rid_t rid_t;

public:
    CIIRegionEncoder(CIIRegionEncoderConfig conf, size_t total_elements);
    virtual ~CIIRegionEncoder() {}

    virtual boost::shared_ptr< CIIRegionEncoding > to_region_encoding();

private:
    void compress_chunk();

    virtual void push_bits_impl(uint64_t count, bool bitval);
    virtual void finalize_impl();

    rid_t next_rid;
    std::vector<rid_t> cur_chunk;
    boost::shared_ptr< CIIRegionEncoding > encoding;
};

