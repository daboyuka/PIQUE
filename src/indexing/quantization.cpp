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
 * quantization.cpp
 *
 *  Created on: Jun 4, 2014
 *      Author: David A. Boyuka II
 */

#include "pique/indexing/quantization.hpp"

using NumericSignednessType = Datatypes::NumericSignednessType;

// Sigbits quantized key compare helper functions
struct SigbitsQuantizationHelper {
	template< NumericSignednessType Signedness >
	static inline bool compare_in_mode(uint64_t key1, uint64_t key2, int sigbits);

	template< NumericSignednessType Signedness >
	static inline uint64_t minmax_key(int sigbits, bool get_max);

	static inline int64_t sign_extend(uint64_t val, int sigbits) {
		const int insigbits = (sizeof(uint64_t) << 3) - sigbits;
		int64_t sval = type_convert<uint64_t, int64_t>(val);
		return (sval << insigbits) >> insigbits; // Force sign extension
	}

	static inline bool compare_1c(int64_t key1, int64_t key2) {
		const int64_t signbit = 1LL << ((sizeof(int64_t) << 3) - 1);
		// Map any ones-complement negative values to one less than their twos-complement equivalent
		// This is to ensure that 0 and -0 are properly ordered. -0 < 0 in 1c, but there is only 0 in 2c,
		// so by subtracting 1 from all negative 2c values, we ensure any negative value is strictly less
		// than every positive value (considering 0 positive).
		if (key1 & signbit) key1 = signbit - key1 - 1;
		if (key2 & signbit) key2 = signbit - key2 - 1;
		return key1 < key2;
	}

	static inline int64_t convert_1c_to_2c(int64_t val1c) {
		const int64_t signbit = 1LL << ((sizeof(int64_t) << 3) - 1);
		if (val1c & signbit)
			return signbit - val1c; // posval = val1c - signbit, val2c = -posval = 2*signbit - posval -> val2c = signbit - val1c
		else
			return val1c;
	}
};

template<> inline bool
SigbitsQuantizationHelper::compare_in_mode<NumericSignednessType::UNSIGNED>(uint64_t key1, uint64_t key2, int sigbits) {
	return key1 < key2;
}

template<> inline bool
SigbitsQuantizationHelper::compare_in_mode<NumericSignednessType::TWOS_COMPLEMENT>(uint64_t key1, uint64_t key2, int sigbits) {
	return sign_extend(key1, sigbits) < sign_extend(key2, sigbits);
}

template<> inline bool
SigbitsQuantizationHelper::compare_in_mode<NumericSignednessType::ONES_COMPLEMENT>(uint64_t key1, uint64_t key2, int sigbits) {
	return compare_1c(sign_extend(key1, sigbits), sign_extend(key2, sigbits));
}

template<> inline uint64_t
SigbitsQuantizationHelper::minmax_key<NumericSignednessType::UNSIGNED>(int sigbits, bool get_max) {
	return get_max ? (1ULL << sigbits) - 1 : 0ULL;
}

template<> inline uint64_t
SigbitsQuantizationHelper::minmax_key<NumericSignednessType::TWOS_COMPLEMENT>(int sigbits, bool get_max) {
	return get_max ? (1ULL << (sigbits - 1)) - 1 : (1ULL << (sigbits - 1));
}

template<> inline uint64_t
SigbitsQuantizationHelper::minmax_key<NumericSignednessType::ONES_COMPLEMENT>(int sigbits, bool get_max) {
	return get_max ? (1ULL << (sigbits - 1)) - 1 : (1ULL << (sigbits)) - 1 ;
}

// Sigbits quantized key compare implementations
template<NumericSignednessType Signedness>
bool SigbitsQuantizedKeyCompare<Signedness>::compare(QKeyType key1, QKeyType key2) const {
	return SigbitsQuantizationHelper::compare_in_mode< Signedness >(key1, key2, sigbits);
}

template<NumericSignednessType Signedness>
auto SigbitsQuantizedKeyCompare<Signedness>::min_key() const -> QKeyType { return SigbitsQuantizationHelper::minmax_key< Signedness >(sigbits, false); }

template<NumericSignednessType Signedness>
auto SigbitsQuantizedKeyCompare<Signedness>::max_key() const -> QKeyType { return SigbitsQuantizationHelper::minmax_key< Signedness >(sigbits, true); }



// Dynamic, semantic value casting, sigbits quantized key compare
auto DynamicSigbitsQuantizedKeyCompare::base_quantized_key_compare_helper(int sigbits, NumericSignednessType signedness)
	-> std::unique_ptr< GenericBaseQuantizedKeyCompareType >
{
	typedef std::unique_ptr< GenericBaseQuantizedKeyCompareType > RetPtr;
	switch (signedness) {
	case NumericSignednessType::UNSIGNED:			return RetPtr(new SigbitsQuantizedKeyCompare<NumericSignednessType::UNSIGNED>(sigbits));
	case NumericSignednessType::ONES_COMPLEMENT:	return RetPtr(new SigbitsQuantizedKeyCompare<NumericSignednessType::ONES_COMPLEMENT>(sigbits));
	case NumericSignednessType::TWOS_COMPLEMENT:	return RetPtr(new SigbitsQuantizedKeyCompare<NumericSignednessType::TWOS_COMPLEMENT>(sigbits));
	default:
		return RetPtr(nullptr);
	}
}

DynamicSigbitsQuantizedKeyCompare::DynamicSigbitsQuantizedKeyCompare(int sigbits, NumericSignednessType original_datatype_signedness) :
		base(base_quantized_key_compare_helper(sigbits, original_datatype_signedness))
{}
DynamicSigbitsQuantizedKeyCompare::DynamicSigbitsQuantizedKeyCompare(int sigbits, std::type_index original_datatype) :
		base(base_quantized_key_compare_helper(sigbits, *Datatypes::get_signedness(original_datatype)))
{}


// Instantiate SigbitsQuantizedKeyCompare for all 3 signedness types so that we can keep its implementation in its own compilation unit
template class SigbitsQuantizedKeyCompare<NumericSignednessType::UNSIGNED>;
template class SigbitsQuantizedKeyCompare<NumericSignednessType::ONES_COMPLEMENT>;
template class SigbitsQuantizedKeyCompare<NumericSignednessType::TWOS_COMPLEMENT>;
