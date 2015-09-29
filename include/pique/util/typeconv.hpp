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
 * typeconv.hpp
 *
 *  Created on: Jan 24, 2014
 *      Author: David A. Boyuka II
 */

#ifndef TYPECONV_HPP_
#define TYPECONV_HPP_

template<typename A, typename B>
union TypeConvert {
public:
	TypeConvert() : in() {}
    B convert(const A &a) { out = B(); in = a; return out; };
public:
    A in;
    B out;
};

template<typename A, typename B>
inline B type_convert(const A& in) {
    TypeConvert<A, B> conv;
    return conv.convert(in);
}

#endif /* TYPECONV_HPP_ */
