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

int main(int argc, char **argv) {
	typedef CBLQSemiwords<2>::cblq_semiword_t cblq_semiword_t;

	CBLQSemiwords<2> sw(false);

	// Extraneous 1's on the end (most significant)
	sw.push((cblq_semiword_t)0b11110011);
	sw.push((cblq_semiword_t)0b11111001);
	sw.push((cblq_semiword_t)0b11110000);
	sw.push((cblq_semiword_t)0b11111111);

	sw.dump();
	printf("\n\n");
	// Expected: 1100 1001 0000 1111

	CBLQSemiwords<2>::iterator it = sw.iterator_at(2);
	it.set(0b0001);
	it.next();
	it.set(0b0010);
	it.next();
	it.set(0b0011);

	sw.dump();
	printf("\n\n");
	// Expected: 1100 1001 1000 0100 1100

	CBLQRegionEncoding<2>::print_cblq_word(sw.iterator_at(0).top_fullword());
	printf("\n\n");
	// Expected: 1100

	CBLQRegionEncoding<2>::print_cblq_word(sw.iterator_at(1).top_fullword());
	printf("\n\n");
	// Expected: 1001

	sw.push(0b1010); // word = 0101
	sw.push_fullword(0b01000100); // word = 0101

	sw.dump();
	printf("\n\n");
	// Expected: 1100 1001 1000 0100 1100 0101 0101

	CBLQSemiwords<2> sw_tail(false);
	for (int i= 0; i < 64; i++)
		sw_tail.push(i & 0b1111);

	sw_tail.dump();
	printf("\n\n");
	// Expected 0000 1000 0100 ... 1111, all repeated 4 times

	sw.append(sw_tail);
	sw.dump();
	printf("\n\n");
	// Expected 1100 1001 1000 0100 1100 0101 0101, then 0000 1000 0100 ... 1111, all repeated 4 times

	CBLQSemiwords<2> sw2(true);

	sw2.push((cblq_semiword_t)0b0011);
	sw2.push((cblq_semiword_t)0b1001);
	sw2.push((cblq_semiword_t)0b0000);
	sw2.push((cblq_semiword_t)0b1111);

	sw2.dump();
	printf("\n\n");
	// Expected: 1100 1001 0000 1111

	CBLQRegionEncoding<2>::print_cblq_word(sw2.iterator_at(0).top_fullword());
	printf("\n\n");
	// Expected: 2200

	CBLQRegionEncoding<2>::print_cblq_word(sw2.iterator_at(1).top_fullword());
	printf("\n\n");
	// Expected: 2002

	printf("Long iterator append test\n");

	CBLQSemiwords<2> swlong(false);
	typename CBLQSemiwords<2>::iterator itlong = swlong.begin();

	for (int i = 0; i < 64; i++) {
		itlong.set((cblq_semiword_t)0b1111);
		itlong.next();
	}

	swlong.dump();
	// Expected:
	// 1111, repeated 64 times
}




