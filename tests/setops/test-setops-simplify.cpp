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
 * test-setops-simplify.cpp
 *
 *  Created on: August 6, 2014
 *      Author: David A. Boyuka II
 */

#include <cstdint>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/dynamic_bitset.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/setops/setops.hpp"

using RU = RegionEncoding::RegionUniformity;
using NSO = NArySetOperation;
using SSO = SimplifiedSetOp;

static void test_simplify(NSO op, std::vector< RU > operands, SSO expected_result) {
	assert(SSO::simplify(op, operands) == expected_result);
}

int main(int argc, char **argv) {
	test_simplify(
		NSO::UNION,
		{ RU::MIXED, RU::MIXED },
		SSO(RU::MIXED, NSO::UNION, false)
	);
	test_simplify(
		NSO::UNION,
		{ RU::FILLED, RU::MIXED },
		SSO(RU::FILLED, NSO::UNION, false)
	);
	test_simplify(
		NSO::UNION,
		{ RU::MIXED, RU::FILLED },
		SSO(RU::FILLED, NSO::UNION, false)
	);
	test_simplify(
		NSO::UNION,
		{ RU::EMPTY, RU::MIXED },
		SSO(RU::MIXED, NSO::UNION, false)
	);
	test_simplify(
		NSO::UNION,
		{ RU::EMPTY, RU::FILLED },
		SSO(RU::FILLED, NSO::UNION, false)
	);

	test_simplify(
		NSO::INTERSECTION,
		{ },
		SSO(RU::FILLED, NSO::INTERSECTION, false)
	);
	test_simplify(
		NSO::INTERSECTION,
		{ RU::MIXED },
		SSO(RU::MIXED, NSO::INTERSECTION, false)
	);
	test_simplify(
		NSO::INTERSECTION,
		{ RU::MIXED, RU::EMPTY, RU::MIXED },
		SSO(RU::EMPTY, NSO::INTERSECTION, false)
	);

	test_simplify(
		NSO::DIFFERENCE,
		{ RU::MIXED },
		SSO(RU::MIXED, NSO::DIFFERENCE, false)
	);
	test_simplify(
		NSO::DIFFERENCE,
		{ RU::EMPTY, RU::MIXED },
		SSO(RU::EMPTY, NSO::DIFFERENCE, false)
	);
	test_simplify(
		NSO::DIFFERENCE,
		{ RU::FILLED, RU::MIXED },
		SSO(RU::MIXED, NSO::UNION, true)
	);

	test_simplify(
		NSO::SYMMETRIC_DIFFERENCE,
		{ RU::FILLED, RU::MIXED },
		SSO(RU::MIXED, NSO::SYMMETRIC_DIFFERENCE, true)
	);
	test_simplify(
		NSO::SYMMETRIC_DIFFERENCE,
		{ RU::FILLED, RU::MIXED, RU::EMPTY, RU::FILLED },
		SSO(RU::MIXED, NSO::SYMMETRIC_DIFFERENCE, false )
	);
}




