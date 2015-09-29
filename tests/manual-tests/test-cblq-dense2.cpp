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
#include "pique/setops/setops.hpp"
#include "pique/setops/cblq/cblq-setops.hpp"

int main(int argc, char **argv) {
    CBLQRegionEncoder<2> builder(CBLQRegionEncoderConfig(true), 256);
    CBLQRegionEncoder<2> builder2(CBLQRegionEncoderConfig(true), 256);
    CBLQSetOperations<2> setops(CBLQSetOperationsConfig(false));

    for (int i = 0; i < 64; i++) {
		builder.push_bits(2, true);
		builder.push_bits(2, false);
		builder2.push_bits(2, false);
		builder2.push_bits(2, true);
    }
	builder.finalize();
	builder2.finalize();

    boost::shared_ptr< CBLQRegionEncoding<2> > region = builder.to_region_encoding();
    boost::shared_ptr< CBLQRegionEncoding<2> > region2 = builder2.to_region_encoding();

    printf("Region 1:\n");
    region->dump();
    printf("\n\n");
    printf("Region 2:\n");
    region2->dump();
    printf("\n\n");


    boost::shared_ptr< CBLQRegionEncoding<2> > uregion = setops.binary_set_op(region, region2, NArySetOperation::UNION);

    printf("Union:\n");
    uregion->dump();
    printf("\n\n");

    printf("Union compacted:\n");
    uregion->compact();
    uregion->dump();
    printf("\n\n");
}




