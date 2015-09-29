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
 * test-quantization.cpp
 *
 *  Created on: May 21, 2014
 *      Author: David A. Boyuka II
 */

#include <cassert>
#include <cstdint>
#include <iostream>
#include <cmath>
#include "pique/indexing/quantization.hpp"

template<typename ValT>
static bool check_compare_sigbits_dynamic(ValT a, int sigbits, uint64_t expected_key) {
	DynamicCastingSigbitsQuantization<double> q(sigbits, Datatypes::CTypeToDatatypeID< ValT >::value);

	uint64_t dynkey = q.quantize((double)a);
	if (dynkey != expected_key) {
		std::cerr << "Dynamic quantization failed for (double)" << a << " -> " << typeid(a).name() << " (got " << dynkey << ", expected " << expected_key << std::endl;
		abort();
	}

	return true;
}

template<typename ValT>
static bool check_sigbits_compare(ValT a, ValT b, int sigbits, bool expected, bool rexpected) {
	BOOST_STATIC_ASSERT(Datatypes::NumericSignednessTypeInfo< ValT >::SIGNEDNESS_TYPE != Datatypes::NumericSignednessType::NOT_APPLICABLE);
	SigbitsQuantization<ValT> q(sigbits);
	SigbitsQuantizedKeyCompare< Datatypes::NumericSignednessTypeInfo< ValT >::SIGNEDNESS_TYPE > qc(sigbits);

	uint64_t keya = q.quantize(a);
	uint64_t keyb = q.quantize(b);
	if (qc.compare(keya, keyb) != expected) {
		std::cerr << "Compare failed for " << a << " < " << b << " at sigbits " << sigbits << " (expected " << expected << ", got " << !expected << ")" << std::endl;
		std::cerr << "(Orig. type: " << typeid(ValT).name() << ", Quantized: " << keya << ", " << keyb << ")" << std::endl;
		abort();
	}

	if (qc.compare(keyb, keya) != rexpected) {
		std::cerr << "Compare failed for " << b << " < " << a << " at sigbits " << sigbits << " (expected " << rexpected << ", got " << !rexpected << ")" << std::endl;
		std::cerr << "(Orig. type: " << typeid(ValT).name() << ", Quantized: " << keyb << ", " << keya << ")" << std::endl;
		abort();
	}

	return true;
}

static void check_sigbits() {
	for (int sigbits = 12; sigbits <= 64; sigbits++) { // Need 12 to hold the sign bit and exponent; mantissa not needed for these
		check_sigbits_compare<double>(1, 2, sigbits, true, false);
		check_sigbits_compare<double>(1, 1, sigbits, false, false);
		check_sigbits_compare<double>(-1, 1, sigbits, true, false);
	}

	//std::cout << "uint16_t tests..." << std::endl;
	for (int sigbits = 2; sigbits <= 16; sigbits++) {
		check_sigbits_compare<uint16_t>(0x4000, 0x8000, sigbits, true, false);
		check_sigbits_compare<uint16_t>(0x4000, 0x4000, sigbits, false, false);
		check_sigbits_compare<uint16_t>(0x0000, 0x4000, sigbits, true, false);
		check_sigbits_compare<uint16_t>(0x8000, 0xC000, sigbits, true, false);
	}

	//std::cout << "int16_t tests..." << std::endl;
	for (int sigbits = 3; sigbits <= 16; sigbits++) {
		check_sigbits_compare<int16_t>(0x2000, 0x4000, sigbits, true, false);
		check_sigbits_compare<int16_t>(0x2000, 0x2000, sigbits, false, false);
		check_sigbits_compare<int16_t>(0x0000, 0x2000, sigbits, true, false);
		check_sigbits_compare<int16_t>(-0x2000, 0x2000, sigbits, true, false);
		check_sigbits_compare<int16_t>(-0x2000, 0x6000, sigbits, true, false);
		check_sigbits_compare<int16_t>(-0x8000, 0x6000, sigbits, true, false);
	}
}

template<typename ValT>
static bool check_precision(ValT in, ValT out_expected, int digits) {
	const ValT EPSILON = in / 1e6;

	PrecisionQuantization< ValT > quant(digits);
	const ValT out = quant.quantize(in);
	assert(std::fabs(out - out_expected) < EPSILON);
	return true;
}

static void check_precision() {
	check_precision<double>(1.234, 1.23, 3);
	check_precision<double>(1.234, 1.2, 2);
	check_precision<double>(1.234, 1, 1);

	check_precision<double>(9.87654321e24, 9.88e24, 3);
}

int main(int argc, char **argv) {
	check_sigbits();
	check_precision();
}



