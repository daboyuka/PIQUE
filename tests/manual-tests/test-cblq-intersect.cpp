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
#include <boost/smart_ptr.hpp>

#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-encode.hpp"
#include "pique/setops/setops.hpp"
#include "pique/setops/cblq/cblq-setops.hpp"

template<typename SetOps>
void do_test() {
    CBLQRegionEncoder<2> builder(CBLQRegionEncoderConfig(true), 64);

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

    boost::shared_ptr< CBLQRegionEncoding<2> > region = builder.to_region_encoding();

    region->dump();
    printf("\n\n");
    // Expected:
    // Level 0: 2121
    // Level 1: 2220 1102
    // Level 2:
    // Dense suffix: 1110 0001 1000 0001

    CBLQRegionEncoder<2> builder2(CBLQRegionEncoderConfig(true), 64);

    builder2.push_bits(7, true);
    builder2.push_bits(2, false);
    builder2.push_bits(7, true);

    builder2.push_bits(16, false);

    builder2.push_bits(32, false);
    builder2.finalize();

    boost::shared_ptr< CBLQRegionEncoding<2> > region2 = builder2.to_region_encoding();

    region2->dump();
    printf("\n\n");
    // Expected:
    // Level 0: 2000
    // Level 1: 1221
    // Level 2:
    // Dense suffix: 1110 0111

    std::vector< boost::shared_ptr< CBLQRegionEncoding<2> > > regions;
    regions.push_back(region);
    regions.push_back(region2);

    SetOps setops(CBLQSetOperationsConfig(false));
    SetOps setops_compact(CBLQSetOperationsConfig(true));

    boost::shared_ptr< const CBLQRegionEncoding<2> > iregion = setops.nary_set_op(regions.begin(), regions.end(), NArySetOperation::INTERSECTION);
    printf("Intersect:\n");
    iregion->dump();
    printf("\n\n");
    // Expected:
    // Level 0: 2000
    // Level 1: 2220
    // Level 2:
    // Dense suffix: 1110 0000 0000

    boost::shared_ptr< const CBLQRegionEncoding<2> > icregion = setops_compact.nary_set_op(regions.begin(), regions.end(), NArySetOperation::INTERSECTION);
    printf("Intersect-compact:\n");
    icregion->dump();
    printf("\n\n");
    // Expected:
    // Level 0: 2000
    // Level 1: 2000
    // Level 2:
    // Dense suffix: 1110
}

int main(int argc, char **argv) {
	//do_test< CBLQSetOperationsNAry2Dense<2> >();
	do_test< CBLQSetOperationsNAry3Dense<2> >();
}




