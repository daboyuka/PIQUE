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
 * dilate.hpp
 *
 * Bitwise integer dilation/undilation. Algorithms courtesy of
 * "Converting to and from Dilated Integers" by Raman and Wise
 * (http://www.cs.indiana.edu/~dswise/Arcee/castingDilated-comb.pdf).
 *
 * Note: currently only 2-dilation is implemented, using the
 * algorithms from Figure 5 of the above paper. It's not the most
 * efficient implementation possible, but it should suffice for now
 * for this codebase.
 *
 *  Created on: Sep 16, 2015
 *      Author: David A. Boyuka II
 */
#ifndef SRC_UTIL_DILATE_HPP_
#define SRC_UTIL_DILATE_HPP_

template<typename word_t, int d>
class Dilater {
public:
	static word_t dilate(word_t word);
	static word_t undilate(word_t word);
};

#include "impl/dilate-impl.hpp"

#endif /* SRC_UTIL_DILATE_HPP_ */
