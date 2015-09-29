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
 * dbprintf.hpp
 *
 *  Created on: Sep 2, 2014
 *      Author: David A. Boyuka II
 */
#ifndef DBPRINTF_HPP_
#define DBPRINTF_HPP_

#include <cstdio>

#ifdef DEBUG
#  define dbprintf(...) fprintf(stderr, __VA_ARGS__)
#else
#  define dbprintf(...) ((void)0)
#endif

#endif /* DBPRINTF_HPP_ */
