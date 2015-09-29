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
 * binning-spec.hpp
 *
 * An AbstractBinningSpecification performs the following functions:
 * > Binds together a Quantization and QuantizedKeyCompare instance to form a consistent method for handling values and qkeys
 * > Provides serialization services for itself, enabling the storage and retrieval of the exact Quantization/QuantizedKeyCompare used to index a dataset
 * > Once "populated", maintains the list of bins actually present in a given dataset, along with functions for finding which bin would contain a given data value
 *   > Before "population", these functions are not usable; only quantization/quantized key compare services are available
 *
 *  Created on: Aug 11, 2014
 *      Author: David A. Boyuka II
 */
#ifndef BINNING_SPEC_HPP_
#define BINNING_SPEC_HPP_

#include <cstdint>

#include <boost/type_traits.hpp>
#include <boost/static_assert.hpp>
#include <boost/none.hpp>
#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/wrapper.hpp>
#include <boost/serialization/nvp.hpp>

#include "pique/util/datatypes.hpp"
#include "pique/util/universal-value.hpp"
#include "pique/util/dynamic-dispatch.hpp"
#include "pique/util/dynamic-pointer-serialization.hpp"
#include "pique/indexing/quantization.hpp"

// Forward declarations for use in the static factory function in AbstractBinningSpecification
template<typename ValueTypeT> class SigbitsBinningSpecification;
template<typename ValueTypeT> class ExplicitBinsBinningSpecification;

class AbstractBinningSpecification {
public:
    typedef size_t bin_count_t; // Number of bins
    typedef bin_count_t bin_id_t; // ID (index) of a bin

    static constexpr bool is_valid_instantiation = true;

    enum struct BinningSpecificationType : char { SIGBITS, EXPLICIT_BINS, PRECISION };
    using BinningUniversalDatatypeID = UniversalValue::Datatype; // the universal datatype used when operating through the AbstractBinningSpecification interface

public:
    AbstractBinningSpecification() : populated(false) {}
    virtual ~AbstractBinningSpecification() {}

    bool is_populated() const { return populated; }
    virtual void depopulate() = 0;

    virtual Datatypes::IndexableDatatypeID get_binning_datatype() const = 0;
    virtual BinningSpecificationType get_binning_spec_type() const = 0;

    virtual bin_count_t get_num_bins() const = 0;
    virtual UniversalValue get_bin_key(bin_id_t bin) const = 0;
    virtual bin_id_t lower_bound_bin(UniversalValue value, bin_id_t start_from = 0) const = 0;
    virtual bin_id_t upper_bound_bin(UniversalValue value, bin_id_t start_from = 0) const = 0;

    BinningUniversalDatatypeID get_binning_universal_datatype() const {return  UniversalValue::convert_datatype_to_universal_datatype(this->get_binning_datatype()); };

    std::vector< UniversalValue > get_all_bin_keys() const {
    	std::vector< UniversalValue > bin_keys;
    	bin_keys.reserve(this->get_num_bins());
    	for (bin_id_t i = 0; i < this->get_num_bins(); ++i)
    		bin_keys.push_back(this->get_bin_key(i));
    	return bin_keys;
    }

protected:
	void set_populated(bool populated) { this->populated = populated; }

private:
	friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	if (Archive::is_loading::value)
    		populated = true;
    	else
    		assert(populated);
    }

private:
	bool populated;
};

template<typename QuantizationT, typename QuantizedKeyCompareT >
class BinningSpecification : public AbstractBinningSpecification {
public:
	using ParentType = AbstractBinningSpecification;
	using QuantizationType = QuantizationT;
	using QuantizedKeyCompareType = QuantizedKeyCompareT;
	using ValueType = typename QuantizationType::ValueType;
	using QKeyType = typename QuantizationType::QKeyType;
	BOOST_STATIC_ASSERT(std::is_same< QKeyType, typename QuantizedKeyCompareType::QKeyType >::value);

	BinningSpecification(QuantizationType quant, QuantizedKeyCompareType qcompare) :
		quant(std::move(quant)), qcompare(std::move(qcompare))
	{}
	virtual ~BinningSpecification() {}

	virtual bin_count_t get_num_bins() const {
		assert(is_populated());
		return bin_qkeys.size();
	}

    virtual UniversalValue get_bin_key(bin_id_t bin) const {
    	assert(is_populated());
    	return UniversalValue(get_bin_key_impl(bin));
    }

    // Returns the ID for the lowest bin X such bin X *is not less than* the bin Y that "value" would fall into (i.e. returnedBin >= valueBin)
    virtual bin_id_t lower_bound_bin(UniversalValue value, bin_id_t start_from = 0) const {
    	return bound_bin(value, true, start_from);
    }

    // Returns the ID for the lowest bin X such bin X *is greater than* the bin Y that "value" would fall into (i.e. returnedBin > valueBin)
    virtual bin_id_t upper_bound_bin(UniversalValue value, bin_id_t start_from = 0) const {
    	return bound_bin(value, false, start_from);
    }

    virtual void populate(std::vector< QKeyType > qkeys) {
    	bin_qkeys = std::move(qkeys);
    	set_populated(true);
    }

    virtual void depopulate() {
    	bin_qkeys.clear();
    	set_populated(false);
    }

    const QuantizationType & get_quantization() const { return quant; }
    const QuantizedKeyCompareType & get_quantized_key_compare() const { return qcompare; }

private:

    bin_id_t bound_bin(UniversalValue user_value, bool inclusive_bound, bin_id_t start_from = 0) const {
    	assert(is_populated());
    	if (!user_value.is_convertible_to< ValueType >())
    		return this->get_num_bins();

    	const ValueType value = user_value.convert_to< ValueType >(); //boost::apply_visitor(CastHelper(), user_value);
    	return bound_bin_impl(value, inclusive_bound, start_from);
    }

	virtual QKeyType get_bin_key_impl(bin_id_t bin) const {
		assert(bin >= 0 && bin < bin_qkeys.size());
		return bin_qkeys[bin];
	}
	virtual bin_id_t bound_bin_impl(ValueType value, bool inclusive_bound, bin_id_t start_from) const {
    	const QKeyType value_qkey = quant.quantize(value);

    	const bin_count_t nbins = get_num_bins();
    	bin_id_t i;
    	for (i = start_from; i < nbins; ++i) {
    		QKeyType bin_qkey = get_bin_key_impl(i);
    		if (!qcompare.compare(bin_qkey, value_qkey)) // bin_qkey >= qkey
    			if (inclusive_bound || bin_qkey != value_qkey) // if inclusive bound, return this bin; if not, only return it if != (i.e., bin_qkey > qkey)
    				break;
    	}

    	return i;
	}

	friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	ar & boost::serialization::base_object<ParentType>(*this);
    	ar & bin_qkeys;
    }

protected:
	QuantizationType quant;
	QuantizedKeyCompareType qcompare;
	std::vector< QKeyType > bin_qkeys;
};

// Sigbits binning concrete implementation

template<typename ValueTypeT>
class SigbitsBinningSpecification :
		public BinningSpecification<
			SigbitsQuantization< ValueTypeT >,
			SigbitsQuantizedKeyCompare< Datatypes::NumericSignednessTypeInfo< ValueTypeT >::SIGNEDNESS_TYPE > >
{
public:
	using ValueType = ValueTypeT;
	using QuantizationType = SigbitsQuantization< ValueType >;
	using QuantizedKeyCompareType = SigbitsQuantizedKeyCompare< Datatypes::NumericSignednessTypeInfo< ValueType >::SIGNEDNESS_TYPE >;
	using ParentType = BinningSpecification< QuantizationType, QuantizedKeyCompareType >;
	using QKeyType = typename ParentType::QKeyType;
	using BinningSpecificationType = typename ParentType::BinningSpecificationType;
	using BinningUniversalDatatypeID = typename ParentType::BinningUniversalDatatypeID;
	static constexpr AbstractBinningSpecification::BinningSpecificationType SpecTypeID = AbstractBinningSpecification::BinningSpecificationType::SIGBITS;
	static constexpr Datatypes::IndexableDatatypeID ValueDatatypeID = Datatypes::CTypeToDatatypeID< ValueType >::value;

	SigbitsBinningSpecification() : ParentType(QuantizationType(0), QuantizedKeyCompareType(0)), sigbits(0) {}
	SigbitsBinningSpecification(int sigbits) :
		ParentType(QuantizationType(sigbits), QuantizedKeyCompareType(sigbits)), sigbits(sigbits)
	{}
	virtual ~SigbitsBinningSpecification() {}

	virtual Datatypes::IndexableDatatypeID get_binning_datatype() const { return ValueDatatypeID; }
	virtual BinningSpecificationType get_binning_spec_type() const { return SpecTypeID; }

	int get_sigbits() const { return sigbits; }
private:
	friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	ar & boost::serialization::base_object<ParentType>(*this);

    	ar & sigbits;
    	if (Archive::is_loading::value) {
    		this->quant = QuantizationType(sigbits);
    		this->qcompare = QuantizedKeyCompareType(sigbits);
    	}
    }

    int sigbits;
};

template<>
class SigbitsBinningSpecification< std::string > {
public:
	static constexpr bool is_valid_instantiation = false;
	using ValueType = std::string;
	static constexpr AbstractBinningSpecification::BinningSpecificationType SpecTypeID = AbstractBinningSpecification::BinningSpecificationType::SIGBITS;
	static constexpr Datatypes::IndexableDatatypeID ValueDatatypeID = Datatypes::CTypeToDatatypeID< ValueType >::value;

	virtual ~SigbitsBinningSpecification() {}

	virtual Datatypes::IndexableDatatypeID get_binning_datatype() const { abort(); return ValueDatatypeID; }
	virtual AbstractBinningSpecification::BinningSpecificationType get_binning_spec_type() const { abort(); return AbstractBinningSpecification::BinningSpecificationType::SIGBITS; }

	int get_sigbits() const { abort(); }
private:
	friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	abort();
    }
};


template<typename ValueTypeT, typename EnableIf = void>
class PrecisionBinningSpecification;

template<typename ValueTypeT>
class PrecisionBinningSpecification< ValueTypeT, typename boost::enable_if< boost::is_floating_point< ValueTypeT > >::type > :
		public BinningSpecification<
			PrecisionQuantization< ValueTypeT >,
			PrecisionQuantization< ValueTypeT > >
{
public:
	using ValueType = ValueTypeT;
	using QuantizationType = PrecisionQuantization< ValueType >;
	using QuantizedKeyCompareType = PrecisionQuantization< ValueType >;
	using ParentType = BinningSpecification< QuantizationType, QuantizedKeyCompareType >;
	using QKeyType = typename ParentType::QKeyType;
	using BinningSpecificationType = typename ParentType::BinningSpecificationType;
	using BinningUniversalDatatypeID = typename ParentType::BinningUniversalDatatypeID;
	static constexpr AbstractBinningSpecification::BinningSpecificationType SpecTypeID = AbstractBinningSpecification::BinningSpecificationType::PRECISION;
	static constexpr Datatypes::IndexableDatatypeID ValueDatatypeID = Datatypes::CTypeToDatatypeID< ValueType >::value;

	PrecisionBinningSpecification() : ParentType(QuantizationType(0), QuantizedKeyCompareType(0)), digits(0) {}
	PrecisionBinningSpecification(int digits) :
		ParentType(QuantizationType(digits), QuantizedKeyCompareType(digits)), digits(digits)
	{}
	virtual ~PrecisionBinningSpecification() {}

	virtual Datatypes::IndexableDatatypeID get_binning_datatype() const { return ValueDatatypeID; }
	virtual BinningSpecificationType get_binning_spec_type() const { return SpecTypeID; }

	int get_digits() const { return digits; }
private:
	friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	ar & boost::serialization::base_object<ParentType>(*this);

    	ar & digits;
    	if (Archive::is_loading::value) {
    		this->quant = QuantizationType(digits);
    		this->qcompare = QuantizedKeyCompareType(digits);
    	}
    }

    int digits;
};

template<typename ValueTypeT>
class PrecisionBinningSpecification< ValueTypeT, typename boost::disable_if< boost::is_floating_point< ValueTypeT > >::type > {
public:
	static constexpr bool is_valid_instantiation = false;
	using ValueType = ValueTypeT;
	static constexpr AbstractBinningSpecification::BinningSpecificationType SpecTypeID = AbstractBinningSpecification::BinningSpecificationType::PRECISION;
	static constexpr Datatypes::IndexableDatatypeID ValueDatatypeID = Datatypes::CTypeToDatatypeID< ValueType >::value;

	virtual ~PrecisionBinningSpecification() {}

	virtual Datatypes::IndexableDatatypeID get_binning_datatype() const { abort(); return ValueDatatypeID; }
	virtual AbstractBinningSpecification::BinningSpecificationType get_binning_spec_type() const { abort(); return AbstractBinningSpecification::BinningSpecificationType::PRECISION; }

	int get_digits() const { abort(); }
private:
	friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	abort();
    }
};



template<typename ValueTypeT>
class ExplicitBinsBinningSpecification :
		public BinningSpecification<
			ExplicitBinsComparableQuantization< ValueTypeT >,
			ExplicitBinsComparableQuantization< ValueTypeT > >
{
public:
	using ValueType = ValueTypeT;
	using QuantizationType = ExplicitBinsComparableQuantization< ValueType >;
	using QuantizedKeyCompareType = ExplicitBinsComparableQuantization< ValueType >;
	using ParentType = BinningSpecification< QuantizationType, QuantizedKeyCompareType >;
	using QKeyType = typename ParentType::QKeyType;
	using BinningSpecificationType = typename ParentType::BinningSpecificationType;
	using BinningUniversalDatatypeID = typename ParentType::BinningUniversalDatatypeID;
	static constexpr AbstractBinningSpecification::BinningSpecificationType SpecTypeID = AbstractBinningSpecification::BinningSpecificationType::EXPLICIT_BINS;
	static constexpr Datatypes::IndexableDatatypeID ValueDatatypeID = Datatypes::CTypeToDatatypeID< ValueType >::value;
	static constexpr BinningUniversalDatatypeID UniversalDatatypeID = UniversalValue::CTypeToDatatype< ValueType >::value;

	static QKeyType get_negative_infinity();

	ExplicitBinsBinningSpecification() :
		ParentType(
				QuantizationType(std::vector< QKeyType >(), get_negative_infinity()),
				QuantizedKeyCompareType(std::vector< QKeyType >(), get_negative_infinity())),
				orig_bin_qkeys()
	{}
	ExplicitBinsBinningSpecification(std::vector< QKeyType > boundary_qkeys) :
		ParentType(
				QuantizationType(boundary_qkeys, get_negative_infinity()),
				QuantizedKeyCompareType(boundary_qkeys, get_negative_infinity())),
				orig_bin_qkeys(std::move(boundary_qkeys))
	{}
	virtual ~ExplicitBinsBinningSpecification() {}

	virtual Datatypes::IndexableDatatypeID get_binning_datatype() const { return ValueDatatypeID; }
	virtual BinningSpecificationType get_binning_spec_type() const { return SpecTypeID; }

	virtual void populate(std::vector< QKeyType > qkeys) {
		ParentType::populate(std::move(qkeys));
		this->quant = QuantizationType(this->bin_qkeys, get_negative_infinity());
		this->qcompare = QuantizedKeyCompareType(this->bin_qkeys, get_negative_infinity());
	}

    virtual void depopulate() {
    	ParentType::depopulate();
		this->quant = QuantizationType(this->orig_bin_qkeys, get_negative_infinity());
		this->qcompare = QuantizedKeyCompareType(this->orig_bin_qkeys, get_negative_infinity());
    }

private:
	friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	ar & boost::serialization::base_object<ParentType>(*this);

    	if (Archive::is_loading::value) {
    		this->quant = QuantizationType(this->bin_qkeys, get_negative_infinity());
    		this->qcompare = QuantizedKeyCompareType(this->bin_qkeys, get_negative_infinity());
    	}
    }

    std::vector< QKeyType > orig_bin_qkeys;
};



template<typename DatatypeT>
struct BinningSpecificationDispatch {
	using BinningType = AbstractBinningSpecification::BinningSpecificationType;
	typedef typename MakeValueToTypeDispatch< BinningType >::
			WithValues< BinningType::SIGBITS, BinningType::EXPLICIT_BINS, BinningType::PRECISION >::
			WithTypes< SigbitsBinningSpecification< DatatypeT >, ExplicitBinsBinningSpecification< DatatypeT >, PrecisionBinningSpecification< DatatypeT > >::type
		type;
};

// SERIALIZATION

/* Dynamic dispatch from datatype and binning type to serializing binning spec */
template<typename Archive, typename BinningSpecT>
typename boost::enable_if< boost::mpl::and_< typename Archive::is_loading, boost::mpl::bool_< BinningSpecT::is_valid_instantiation > >, bool >::type
serialize_binning_spec_helper(Archive &ar, boost::shared_ptr< const AbstractBinningSpecification > &base_ptr) {
	boost::shared_ptr< BinningSpecT > deriv_ptr = boost::make_shared< BinningSpecT >();
	ar & *deriv_ptr;
	base_ptr = boost::const_pointer_cast< const AbstractBinningSpecification >(boost::static_pointer_cast< AbstractBinningSpecification >(deriv_ptr));
	return true;
}

template<typename Archive, typename BinningSpecT>
typename boost::enable_if< boost::mpl::and_< typename Archive::is_saving, boost::mpl::bool_< BinningSpecT::is_valid_instantiation > >, bool >::type
serialize_binning_spec_helper(Archive &ar, boost::shared_ptr< const AbstractBinningSpecification > &base_ptr) {
	boost::shared_ptr< const BinningSpecT > deriv_ptr = boost::dynamic_pointer_cast< const BinningSpecT >(base_ptr);
	ar & *deriv_ptr;
	return true;
}

template<typename Archive, typename BinningSpecT>
typename boost::disable_if_c< BinningSpecT::is_valid_instantiation, bool >::type
serialize_binning_spec_helper(Archive &ar, boost::shared_ptr< const AbstractBinningSpecification > &base_ptr) { abort(); }

template<typename Archive>
struct SerializeHelper {
	using BinningType = AbstractBinningSpecification::BinningSpecificationType;
	using DTID = typename UniversalValue::Datatype;

	template<typename DatatypeT>
	struct SecondPass {
		template<typename BinningSpecT>
		bool operator()(Archive &ar, boost::shared_ptr< const AbstractBinningSpecification > &base_ptr) {
			serialize_binning_spec_helper< Archive, BinningSpecT >(ar, base_ptr);
			return true;
		}
	};

	template<typename DatatypeT>
	bool operator()(Archive &ar, boost::shared_ptr< const AbstractBinningSpecification > &base_ptr, BinningType &binning_type) {
		using SecondPassDispatch = typename BinningSpecificationDispatch< DatatypeT >::type;
		return SecondPassDispatch::template dispatchMatching< bool >(binning_type, SecondPass< DatatypeT >(), false, ar, base_ptr);
	}
};

template<typename Archive>
void serialize_binning_type(Archive &ar, boost::shared_ptr< const AbstractBinningSpecification > &base_ptr) {
	using BinningType = AbstractBinningSpecification::BinningSpecificationType;
	using DTID = Datatypes::IndexableDatatypeID;

	DTID dtid;
	BinningType binning_type;
	if (Archive::is_saving::value) {
		dtid = base_ptr->get_binning_datatype();
		binning_type = base_ptr->get_binning_spec_type();
	}

	ar & dtid & binning_type;
	const bool success =
			Datatypes::DatatypeIDToCTypeDispatch::template dispatchMatching< bool >(
			dtid, SerializeHelper< Archive >(), false,
			ar, base_ptr, binning_type);

	BOOST_ASSERT(success);
}

/*
struct BinningSpecificationDynamicHelper {
	using BinningType = AbstractBinningSpecification::BinningSpecificationType;
	using DTID = Datatypes::IndexableDatatypeID;

	template<typename ValueTypeT>
	struct ByValueType {
		typedef typename MakeValueToTypeDispatch< BinningType >::
				WithValues< BinningType::SIGBITS, BinningType::EXPLICIT_BINS >::
				WithTypes< SigbitsBinningSpecification< ValueTypeT >, ExplicitBinsBinningSpecification< ValueTypeT > >::type
			Dispatch;

		typedef SerializeDynamicPointer< AbstractBinningSpecification, Dispatch, char > SerializePointer;
	};

private:
	template<typename Archive>
	struct SerializeHelper {
		template<typename DatatypeT>
		bool operator()(Archive &ar, boost::shared_ptr< const AbstractBinningSpecification > &base_ptr, BinningType &binning_type) {
			using InnerSerializeHelper = typename ByValueType< DatatypeT >::SerializePointer;
			InnerSerializeHelper::serialize_pointer(ar, base_ptr, binning_type);
			return true;
		}
	};

public:
	template<typename Archive>
	static void serialize_pointer(Archive &ar, boost::shared_ptr< const AbstractBinningSpecification > &base_ptr, DTID &datatype, BinningType &binning_type) {
		// Save/load the actual pointer type
    	ar & reinterpret_cast<char&>(datatype);

    	// Save/load the actual content of the pointer
    	const bool success =
    		Datatypes::DatatypeIDToCTypeDispatch::template dispatchMatching< bool >(
    			datatype,
    			SerializeHelper< Archive >(), false,
    			ar, base_ptr, binning_type);

    	BOOST_ASSERT(success); // Ensure we successfully matched the key value to some type
	}

	class SerializePointer : public boost::serialization::wrapper_traits< const SerializePointer  > {
	public:
		SerializePointer(boost::shared_ptr< const AbstractBinningSpecification > &base_ptr, DTID &datatype, BinningType &binning_type) :
			base_ptr(base_ptr), datatype(datatype), binning_type(binning_type)
		{}

	private:
		template<typename Archive>
		struct SerializeHelper {
			template<typename DatatypeT>
			bool operator()(Archive &ar, boost::shared_ptr< const AbstractBinningSpecification > &base_ptr, BinningType &binning_type) {
				ar & ByValueType< DatatypeT >::SerializePointer(base_ptr, binning_type);
				return true;
			}
		};

	public:
		friend class boost::serialization::access;
	    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
	    	// Save/load the actual pointer type
	    	ar & reinterpret_cast<char&>(this->datatype);

	    	// Save/load the actual content of the pointer
	    	const bool success =
	    		Datatypes::DatatypeIDToCTypeDispatch::template dispatchMatching< bool >(
	    			this->datatype,
	    			SerializeHelper< Archive >(), false,
	    			ar, this->base_ptr, this->binning_type);

	    	BOOST_ASSERT(success); // Ensure we successfully matched the key value to some type
	    }

	private:
		boost::shared_ptr< const AbstractBinningSpecification > &base_ptr;
		DTID &datatype;
		BinningType &binning_type;
	};
};
*/

#endif /* BINNING_SPEC_HPP_ */
