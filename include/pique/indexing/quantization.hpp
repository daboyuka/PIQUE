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
 * quantization.hpp
 *
 *  Created on: May 16, 2014
 *      Author: David A. Boyuka II
 */
#ifndef QUANTIZATION_HPP_
#define QUANTIZATION_HPP_

#include <memory>
#include <typeindex>

#include <boost/smart_ptr.hpp>
#include <boost/type_traits.hpp>

#include "pique/util/datatypes.hpp"

//
// Basic quantization and quantized key comparison
//
template<typename ValueT, typename QKeyT>
class Quantization {
public:
	typedef ValueT ValueType;
	typedef QKeyT QKeyType;
public:
	virtual ~Quantization() {}
	virtual QKeyType quantize(ValueType val) const = 0;

	QKeyType operator()(const ValueType &val) const { return this->quantize(val); }
};

template<typename QKeyT>
class QuantizedKeyCompare {
public:
	typedef QKeyT QKeyType;
public:
	virtual ~QuantizedKeyCompare() {}
	virtual bool compare(QKeyType key1, QKeyType key2) const = 0;
	bool operator()(const QKeyType &key1, const QKeyType &key2) const { return this->compare(key1, key2); }

	virtual QKeyType min_key() const = 0;
	virtual QKeyType max_key() const = 0;
};

//
// Integral quantized key specialization
//
template<typename ValueT>
class IntegralTypeQuantization : public Quantization<ValueT, uint64_t> {
public:
	typedef typename Quantization<ValueT, uint64_t>::ValueType ValueType;
	typedef typename Quantization<ValueT, uint64_t>::QKeyType QKeyType;
};

class IntegralTypeQuantizedKeyCompare : public QuantizedKeyCompare<uint64_t> {
public:
	typedef typename QuantizedKeyCompare<uint64_t>::QKeyType QKeyType;
};

//
// Sigbits-based value binning, integral quantized key specialization
//
template<typename ValueT>
class SigbitsQuantization : public IntegralTypeQuantization<ValueT> {
public:
	typedef typename IntegralTypeQuantization<ValueT>::ValueType ValueType;
	typedef typename IntegralTypeQuantization<ValueT>::QKeyType QKeyType;
public:
	SigbitsQuantization(int sigbits) : sigbits(sigbits) {}
	virtual ~SigbitsQuantization() {}

	virtual QKeyType quantize(ValueType val) const;
protected:
	int sigbits;
};

template<Datatypes::NumericSignednessType Signedness>
class SigbitsQuantizedKeyCompare : public IntegralTypeQuantizedKeyCompare {
public:
	typedef IntegralTypeQuantizedKeyCompare::QKeyType QKeyType;
public:
	SigbitsQuantizedKeyCompare(int sigbits) : sigbits(sigbits) {}
	virtual ~SigbitsQuantizedKeyCompare() {}

	virtual bool compare(uint64_t key1, uint64_t key2) const;
	virtual uint64_t min_key() const;
	virtual uint64_t max_key() const;
protected:
	int sigbits;
};

//
// Precision-based value binning

template<typename ValueT, typename EnableIf = typename boost::enable_if< boost::is_floating_point< ValueT > >::type >
class PrecisionQuantization : public Quantization< ValueT, ValueT >, public QuantizedKeyCompare< ValueT > {
public:
	using ValueType = ValueT;
	using QKeyType = ValueT;
public:
	PrecisionQuantization(int digits) : digits(digits) {}
	virtual ~PrecisionQuantization() {}

	using Quantization< ValueT, ValueT >::operator();
	using QuantizedKeyCompare< ValueT >::operator();

	virtual QKeyType quantize(ValueType val) const;
	virtual bool compare(ValueType key1, ValueType key2) const { return key1 < key2; }
	virtual ValueType min_key() const { return -std::numeric_limits< ValueType >::infinity(); }
	virtual ValueType max_key() const { return std::numeric_limits< ValueType >::infinity(); }
protected:
	int digits;
};

// Explicit bins-based value binning
template<typename ValueT>
class ExplicitBinsComparableQuantization : public Quantization< ValueT, ValueT >, public QuantizedKeyCompare< ValueT > {
public:
	using ValueType = ValueT;
	using QKeyType = ValueT;
public:
	ExplicitBinsComparableQuantization(std::vector< ValueType > bins_by_value, ValueType neg_infinity) :
		neg_infinity(neg_infinity),
		bins(std::move(bins_by_value))
	{}
	virtual ~ExplicitBinsComparableQuantization() {};

	using Quantization< ValueT, ValueT >::operator();
	using QuantizedKeyCompare< ValueT >::operator();

	virtual QKeyType quantize(ValueType val) const;
	virtual bool compare(ValueType key1, ValueType key2) const { return key1 < key2; }
	virtual ValueType min_key() const { return neg_infinity; }
	virtual ValueType max_key() const { return bins.back(); }

	const std::vector< ValueType > & get_bin_bounds() const { return bins; }
	ValueType get_neg_infinity() const { return neg_infinity; }

private:
	ValueType neg_infinity;
	std::vector< ValueType > bins; // only used if we're given a bin vector to keep; otherwise, empty
};

// Semantic value casting quantization
template<typename ValueT, typename BaseQuantizationT>
class CastingQuantization : public Quantization< ValueT, typename BaseQuantizationT::QKeyType > {
public:
	typedef BaseQuantizationT BaseQuantizationType;
	typedef ValueT ValueType;
	typedef typename BaseQuantizationType::ValueType IntermediateValueType;
	typedef typename BaseQuantizationType::QKeyType QKeyType;
public:
	CastingQuantization(BaseQuantizationType &&q) : q(q) {}
	virtual ~CastingQuantization() {}
	virtual QKeyType quantize(ValueType val) const;

private:
	BaseQuantizationType q;
};

template<typename InValueT>
class DynamicCastingSigbitsQuantization : IntegralTypeQuantization<InValueT> {
public:
	typedef InValueT ValueType;
	typedef typename IntegralTypeQuantization<InValueT>::QKeyType QKeyType;
public:
	DynamicCastingSigbitsQuantization(int sigbits, Datatypes::IndexableDatatypeID semantic_cast_to);
	virtual ~DynamicCastingSigbitsQuantization() {}

	virtual QKeyType quantize(ValueType val) const { return base->quantize(val); }

private:
	typedef Quantization< ValueType, QKeyType > GenericBaseQuantizationType;
	std::unique_ptr< GenericBaseQuantizationType > base;

	struct CastingSigbitsQuantizationCreateFn;
};

class DynamicSigbitsQuantizedKeyCompare : IntegralTypeQuantizedKeyCompare {
public:
	typedef typename IntegralTypeQuantizedKeyCompare::QKeyType QKeyType;
public:
	DynamicSigbitsQuantizedKeyCompare(int sigbits, Datatypes::NumericSignednessType original_datatype_signedness);
	DynamicSigbitsQuantizedKeyCompare(int sigbits, std::type_index original_datatype);
	virtual ~DynamicSigbitsQuantizedKeyCompare() {}

	virtual bool compare(uint64_t key1, uint64_t key2) const { return base->compare(key1, key2); }
	virtual uint64_t min_key() const { return base->min_key(); }
	virtual uint64_t max_key() const { return base->max_key(); }

private:
	typedef QuantizedKeyCompare< QKeyType > GenericBaseQuantizedKeyCompareType;
	static std::unique_ptr< GenericBaseQuantizedKeyCompareType > base_quantized_key_compare_helper(int sigbits, Datatypes::NumericSignednessType signedness);
	std::unique_ptr< GenericBaseQuantizedKeyCompareType > base;
};

#include "impl/quantization-impl.hpp"

#endif /* QUANTIZATION_HPP_ */
