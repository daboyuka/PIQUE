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
 * compute-exp-level.hpp
 *
 *  Created on: May 28, 2014
 *      Author: David A. Boyuka II
 */
#ifndef COMPUTE_EXP_LEVEL_HPP_
#define COMPUTE_EXP_LEVEL_HPP_

// Computes the smallest integer "exp_level" such that:
//   2^(exp_level * ndim) >= total_elements
template<int ndim>
static inline int compute_exp_level(size_t total_elements) {
	int exp_level = 0; // 2^(exp_level * ndim) elements
	while (((size_t)1) << (exp_level * ndim) < total_elements)
		exp_level++;

	return exp_level;
}

#endif /* COMPUTE_EXP_LEVEL_HPP_ */
