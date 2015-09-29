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
 * standard-datasets.hpp
 *
 *  Created on: Sep 24, 2014
 *      Author: David A. Boyuka II
 */
#ifndef STANDARD_DATASETS_HPP_
#define STANDARD_DATASETS_HPP_

#include <vector>
#include <cstdlib>

static std::vector< int > make_big_domain(uint64_t domain_size, unsigned int range, int seed = 12345) {
	std::srand(seed);
	std::vector< int > domain;

	uint64_t i = 0;
	for (; i < range; i++)
		domain.push_back(i); // Ensure each key appears at least once
	for (; i < domain_size; i++)
		domain.push_back(std::rand() % range); // Fill the rest with randomness

	return domain;
}

static const std::vector<int> SMALL_DOMAIN = {0, 0, 0, 2, 1, 1, 1, 0, 2, 2, 2, 1, 0, 0, 1, 0};
static const std::vector<int> MEDIUM_DOMAIN = {
		0, 0, 0, 2, 1, 1, 1, 0,
		2, 2, 2, 1, 0, 0, 1, 0,
		3, 3, 3, 3, 2, 3, 3, 3,
		3, 3, 4, 4, 3, 3, 4, 4,
		5, 5, 5, 5, 5, 5, 5, 5,
		5, 5, 5, 5, 5, 5, 5, 5,
		5, 5, 5, 5, 5, 5, 5, 5,
		5, 5, 5, 5, 5, 5, 5, 5,
};
static const std::vector<int> BIG_DOMAIN		= make_big_domain(1ULL << 14, 10);
static const std::vector<int> MANYBINS_DOMAIN	= make_big_domain(1ULL << 12, 200);

#endif /* STANDARD_DATASETS_HPP_ */
