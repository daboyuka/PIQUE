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
 * cblq-build.hpp
 *
 *  Created on: Jan 17, 2014
 *      Author: David A. Boyuka II
 */

#ifndef CBLQ_BUILD_HPP_
#define CBLQ_BUILD_HPP_

extern "C" {
#include <stdint.h>
#include <stdio.h>
}

#include <cassert>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/cblq/cblq.hpp"

struct CBLQRegionEncoderConfig {
	CBLQRegionEncoderConfig(bool encode_dense_suffix) :
		encode_dense_suffix(encode_dense_suffix)
	{}

	bool encode_dense_suffix;
};

template<int ndim>
class CBLQRegionEncoder : public RegionEncoder< CBLQRegionEncoding<ndim>, CBLQRegionEncoderConfig > {
public:
	typedef typename CBLQWord<ndim>::cblq_word_t cblq_word_t;

public:
    CBLQRegionEncoder(CBLQRegionEncoderConfig conf, size_t total_elements);
    CBLQRegionEncoder(CBLQRegionEncoder &&) noexcept = default; // Explicit default move constructor
    virtual ~CBLQRegionEncoder() {}

    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > to_region_encoding();

private:
    virtual void push_bits_impl(uint64_t count, bool bitval);

    const int nlayers;
    const uint64_t nelem;

    uint64_t word_count;
    std::vector< std::vector<cblq_word_t> > layer_words;
    CBLQSemiwords<ndim> dense_suffix_semiwords;

    std::vector<cblq_word_t> cur_words;
    std::vector<uint64_t> cur_word_lens;
};

#endif /* CBLQ_BUILD_HPP_ */
