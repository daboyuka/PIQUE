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
 * test-cache-ptr.cpp
 *
 *  Created on: Jan 8, 2015
 *      Author: David A. Boyuka II
 */

#include <cassert>

#include "pique/util/cache-ptr.hpp"

using sptr = boost::shared_ptr< int >;
using wptr = boost::weak_ptr< int >;
using cptr = boost::cache_ptr< int >;

static sptr makestrong() { return boost::make_shared< int >(123); }

int main(int argc, char **argv) {
	sptr s1 = makestrong();
	cptr c1 = s1;

	assert(s1); assert(c1.is_strong()); assert(!c1.expired());
	s1.reset();
	assert(!s1); assert(c1.is_strong()); assert(!c1.expired());
	c1.weaken();
	assert(!s1); assert(!c1.is_strong()); assert(c1.expired());
	c1.strengthen();
	assert(!s1); assert(!c1.is_strong()); assert(c1.expired());

	s1 = makestrong();
	wptr w1 = s1;
	c1 = w1;

	assert(s1); assert(!w1.expired()); assert(!c1.is_strong()); assert(!c1.expired());
	s1.reset();
	assert(!s1); assert(w1.expired()); assert(!c1.is_strong()); assert(c1.expired());

	s1 = makestrong();
	c1 = s1;
	c1.weaken();
	sptr s2 = c1.lock();

	assert(s1); assert(s2); assert(!c1.is_strong()); assert(!c1.expired());
	s1.reset();
	assert(!s1); assert(s2); assert(!c1.is_strong()); assert(!c1.expired());
	c1.strengthen();
	assert(!s1); assert(s2); assert(c1.is_strong()); assert(!c1.expired());
	s2.reset();
	assert(!s1); assert(!s2); assert(c1.is_strong()); assert(!c1.expired());
	c1.weaken();
	assert(!s1); assert(!s2); assert(!c1.is_strong()); assert(c1.expired());

	s1 = makestrong();
	c1 = s1;
	c1.weaken();
	cptr c2 = c1;
	s1.reset();
	assert(!s1 && !c1.is_strong() && !c2.is_strong() && c1.expired() && c2.expired());
}
