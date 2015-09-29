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
 * test-mask-values.c
 *
 *  Created on: Dec 4, 2013
 *      Author: David A. Boyuka II
 */

#include <cstdio>
#include <vector>

#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-encode.hpp"

typedef std::vector<uint8_t>::iterator cblq_word_iter;

void test_overflow_or() {
	CBLQRegionEncoder<2> builder(CBLQRegionEncoderConfig(false), 64);

    builder.push_bits(10, true);
    builder.push_bits(10, false);
    builder.push_bits(10, true);
    builder.push_bits(10, false);
    builder.push_bits(10, true);
    builder.push_bits(10, false);
    builder.push_bits(4, true);
    builder.finalize();

    std::vector<uint8_t> words;
    boost::shared_ptr< CBLQRegionEncoding<2> > region = builder.to_region_encoding();

    region->dump();
    printf("\n\n");
    // Expected:
    // 2222
    // 1120 0112 0011 2001
    // 1100 1100 1100
}

int main(int argc, char **argv) {
    CBLQRegionEncoder<2> builder(CBLQRegionEncoderConfig(false), 64);

    builder.push_bits(3, true);
    builder.push_bits(4, false);
    builder.push_bits(2, true);
    builder.push_bits(7, false);

    builder.push_bits(24, true);
    builder.push_bits(7, false);
    builder.push_bits(1, true);

    // Try consecutive 1-pushes
    builder.push_bits(16, true);
    builder.finalize();

    std::vector<uint8_t> words;
    boost::shared_ptr< CBLQRegionEncoding<2> > region = builder.to_region_encoding();

    region->dump();
    printf("\n\n");

    test_overflow_or();
}




