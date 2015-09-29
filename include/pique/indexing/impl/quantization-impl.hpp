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
 * quantization-impl.hpp
 *
 *  Created on: Jun 4, 2014
 *      Author: David A. Boyuka II
 */
#ifndef QUANTIZATION_IMPL_HPP_
#define QUANTIZATION_IMPL_HPP_

#include <memory>

#include "pique/util/typeconv.hpp"
#include "pique/util/dynamic-dispatch.hpp"

#include "pique/util/precision.hpp"

// Sigbits quantization implementation
template<typename ValueT>
auto SigbitsQuantization<ValueT>::quantize(ValueType val) const -> QKeyType {
	const int insigbits = (sizeof(ValueType) << 3) - this->sigbits;
	QKeyType valbits = type_convert<ValueType, QKeyType>(val); // TODO: Endianness problem
	return valbits >> insigbits;
}

template<typename ValueT, typename EnableIf>
auto PrecisionQuantization<ValueT, EnableIf>::quantize(ValueType val) const -> QKeyType {
	return ibis::util::coarsen_double(val, this->digits);
}

// Explicit bins-based quantization/quantized key compare implementation
template<typename ValueT>
auto ExplicitBinsComparableQuantization<ValueT>::quantize(ValueType val) const -> QKeyType {
	// Find lowest boundary that is strictly greater than this value to find the exclusive upper-bound boundary...
	auto it = std::upper_bound(this->bins.begin(), this->bins.end(), val, *this /* use *this as QuantizedKeyCompare */);

	// ...then step back one boundary to find the inclusive lower-bound boundary

	if (it != this->bins.begin())
		return *--it;
	else
		return this->neg_infinity;
}

// Semantic value casting quantization implementation
template<typename ValueT, typename QuantizationT>
auto CastingQuantization<ValueT, QuantizationT>::quantize(ValueType val) const -> QKeyType {
	return this->q.quantize(static_cast<IntermediateValueType>(val));
};

// Dynamic, semantic value casting, sigbits quantization
// This class works by using a semantic casting (ConvenientType --semantic cast--> ValueType --sigbits quant--> QKeyType),
// but by dynamically achieving the intermediate casting step via polymorphism. The constructor instantiates a
// statically-typed CastingQuantization wrapping a SigbitsQuantization with the appropriate intermediate type given the
// specified type_index. This Quantization is stored via pointer for runtime polymorphism thereafter, thus invoking the
// dynamic type visitation process only once.
//
template<typename InValueT>
struct DynamicCastingSigbitsQuantization<InValueT>::CastingSigbitsQuantizationCreateFn {
	typedef typename DynamicCastingSigbitsQuantization< InValueT >::GenericBaseQuantizationType GenericBaseQuantizationType;

	template<typename IntermediateTypeT>
	std::unique_ptr< GenericBaseQuantizationType > operator()(int sigbits) {
		typedef SigbitsQuantization< IntermediateTypeT > UnderlyingQuantizationT;
		typedef CastingQuantization< InValueT, UnderlyingQuantizationT > CastingQuantizationT;
		return std::unique_ptr< GenericBaseQuantizationType >(new CastingQuantizationT(UnderlyingQuantizationT(sigbits)));
	}
};

template<typename InValueT>
DynamicCastingSigbitsQuantization<InValueT>::DynamicCastingSigbitsQuantization(int sigbits, Datatypes::IndexableDatatypeID semantic_cast_to) {
	this->base =
			Datatypes::DatatypeIDToCTypeDispatch::dispatchMatching< std::unique_ptr< GenericBaseQuantizationType > >(
				semantic_cast_to,
				CastingSigbitsQuantizationCreateFn(),
				nullptr,
				sigbits
			);

	assert(this->base);
}

#endif /* QUANTIZATION_IMPL_HPP_ */
