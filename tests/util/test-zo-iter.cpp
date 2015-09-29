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
 * test-zo-iter.cpp
 *
 *  Created on: Jan 16, 2014
 *      Author: David A. Boyuka II
 */

#include <cassert>
#include <cstdint>

//#include <zo-iter.hpp>
#include "pique/util/zo-iter2.hpp"

//void do_iter_test(ZOIter &it, uint64_t *expected, uint64_t expected_len) {
//    for (uint64_t i = 0; i < expected_len; i++) {
//        it.next();
//        assert(it.has_top());
//        assert(it.get_top_zid() == i);
//        assert(it.get_top_rmoid() == *expected++);
//    }
//
//    it.next();
//    assert(!it.has_top());
//}

static const uint64_t expected_zids1[] = {0, 1, 2, 3, 4, 6, 8, 9, 12};
static const uint64_t expected_rmoids1[] = {0, 1, 5, 6, 2, 7, 10, 11, 12};
static const uint64_t expected_x1[] = {0, 0, 1, 1, 0, 1, 2, 2, 2};
static const uint64_t expected_y1[] = {0, 1, 0, 1, 2, 2, 0, 1, 2};

static const uint64_t expected_zids2[] = {0, 1, 2, 3, 8, 9, 10, 11};
static const uint64_t expected_rmoids2[] = {0, 1, 4, 5, 8, 9, 12, 13};
static const uint64_t expected_x2[] = {0, 0, 1, 1, 2, 2, 3, 3};
static const uint64_t expected_y2[] = {0, 1, 0, 1, 0, 1, 0, 1};

struct Iter2Test {
	Iter2Test(const uint64_t expected_zids[], const uint64_t expected_rmoids[], const uint64_t expected_x[], const uint64_t expected_y[]) :
		i(0),
		expected_zids(expected_zids),
		expected_rmoids(expected_rmoids),
		expected_x(expected_x),
		expected_y(expected_y)
	{}

	void operator()(uint64_t zid, uint64_t rmoid, uint64_t dims[2]) {
		assert(zid == expected_zids[i]);
		assert(rmoid == expected_rmoids[i]);
		assert(dims[0] == expected_y[i]);
		assert(dims[1] == expected_x[i]);
#ifdef VERBOSE_TESTS
		printf("ZOIter2: zid(%llu), rmoid(%llu)\n", zid, rmoid);
#endif
		i++;
	}

	int i;
	const uint64_t *expected_zids;
	const uint64_t *expected_rmoids;
	const uint64_t *expected_x;
	const uint64_t *expected_y;
};

int main(int argc, char **argv) {
    uint64_t dims[] = { 5, 5 };
    uint64_t subdims[] = { 3, 3 };
//    grid_t *grid    = lin_new_grid(2, dims, true);
//    grid_t *subgrid = lin_new_grid(2, subdims, true);

//    ZOIter it(grid, subgrid);
//    uint64_t exp[] = {0, 1, 5, 6, 2, ~0ULL, 7, ~0ULL, 10, 11, ~0ULL, ~0ULL, 12, ~0ULL, ~0ULL, ~0ULL};
//    do_iter_test(it, exp, 16);

    ZOIterLoop<2/*ndim*/, 3/*exp_level*/> loop;
    loop.iterate(dims, subdims, true, Iter2Test(expected_zids1, expected_rmoids1, expected_x1, expected_y1));

    uint64_t dims2[] = { 4, 4 };
    uint64_t subdims2[] = { 4, 2 };
    ZOIterLoop<2/*ndim*/, 2/*exp_level*/> loop2;
    loop2.iterate(dims2, subdims2, true, Iter2Test(expected_zids2, expected_rmoids2, expected_x2, expected_y2));
}



