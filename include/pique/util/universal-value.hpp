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
 * universal-value.hpp
 *
 * A union-like structure that can contain any basic, indexable datatype. All concrete C types
 * in a class of datatypes are reduced to a single C type capable of representing any C type
 * in that class. The broad classes of datatypes accepted, and their mapped representing types, are:
 *
 * - unsigned integer -> uint64_t
 * - signed integer -> int64_t
 * - floating point value -> double
 * - string -> std::string
 *
 *  Created on: Aug 13, 2014
 *      Author: David A. Boyuka II
 */
#ifndef UNIVERSAL_VALUE_HPP_
#define UNIVERSAL_VALUE_HPP_

#include <string>

#include <boost/variant.hpp>
#include <boost/type_traits.hpp>
#include <boost/none.hpp>
#include <boost/optional.hpp>
#include "pique/util/datatypes.hpp"
#include "pique/util/dynamic-dispatch.hpp"

typedef boost::variant< uint64_t, int64_t, double, std::string > UniversalTypesVariant;

class UniversalValue : public UniversalTypesVariant {
public:
	using ParentVariant = UniversalTypesVariant;
	enum struct Datatype : char {
		UNDEFINED		= (char)Datatypes::IndexableDatatypeID::UNDEFINED,
		UNSIGNED_INT	= (char)Datatypes::IndexableDatatypeID::UINT_64,
		SIGNED_INT		= (char)Datatypes::IndexableDatatypeID::SINT_64,
		FLOATING_POINT	= (char)Datatypes::IndexableDatatypeID::DOUBLE,
		STRING			= (char)Datatypes::IndexableDatatypeID::STRING
	};

	template<Datatype TypeID> struct UniversalDatatypeToDatatype { static constexpr Datatypes::IndexableDatatypeID value = (Datatypes::IndexableDatatypeID)TypeID; };

	template<Datatype TypeID> struct DatatypeToUniversalCType { using type = typename Datatypes::IndexableDatatypeInfo< UniversalDatatypeToDatatype< TypeID >::value >::type; };
	template<typename datatype_t> struct UniversalCTypeToDatatype { static constexpr Datatype value = (Datatype)Datatypes::CTypeToDatatypeID< datatype_t >::value; };

	template<typename datatype_t, typename EnableIf = void> struct CTypeToUniversalCType {};
	template<typename datatype_t> struct CTypeToDatatype { static constexpr Datatype value = UniversalCTypeToDatatype< typename CTypeToUniversalCType< datatype_t >::type >::value; };

private:
	template<typename UserValueT>
	class CastHelper : public boost::static_visitor< boost::optional< UserValueT > > {
	public:
		template<typename ValueT>
		boost::optional< UserValueT > operator()(ValueT &vt, typename boost::disable_if< boost::is_convertible< ValueT, UserValueT >, int >::type ignore = 0) const {
			// Implementation for an invalid cast: fail at runtime
			return boost::none;
		}
		template<typename ValueT>
		boost::optional< UserValueT > operator()(ValueT &vt, typename boost::enable_if< boost::is_convertible< ValueT, UserValueT >, int >::type ignore = 0) const {
			 // Implementation for an valid cast: cast
			return boost::optional< UserValueT >(static_cast< UserValueT >(vt));
		}
	};

	class ToStringHelper : public boost::static_visitor< std::string > {
	public:
		template<typename ValueT>
		std::string operator()(ValueT v) const { return std::to_string(v); }
		std::string operator()(std::string v) const { return v; }
	};

private:
	struct DatatypeToUniversalDatatypeHelper {
		using DTID = Datatypes::IndexableDatatypeID;
		using UnivDTID = UniversalValue::Datatype;
		template< DTID dtid > UnivDTID operator()() { return CTypeToDatatype< typename Datatypes::IndexableDatatypeInfo< dtid >::type >::value; }
	};

public:
	static Datatype convert_datatype_to_universal_datatype(Datatypes::IndexableDatatypeID dtid) {
		return Datatypes::DatatypeIDDispatch::template dispatchMatching< Datatype >(dtid, DatatypeToUniversalDatatypeHelper(), Datatype::UNDEFINED);
	}

	template<typename UserValueT>
	bool is_convertible_to() { return boost::apply_visitor<>(CastHelper< UserValueT >(), *this); }

	template<typename UserValueT>
	UserValueT convert_to() { return *boost::apply_visitor<>(CastHelper< UserValueT >(), *this); }

	std::string to_string() { return boost::apply_visitor<>(ToStringHelper(), *this); }

    bool operator==(const UniversalValue& rhs) const {
    	return this->ParentVariant::operator==(static_cast< const ParentVariant & >(rhs));
    }

    bool operator<(const variant& rhs) const {
    	return this->ParentVariant::operator<(static_cast< const ParentVariant & >(rhs));
    }

public:
	UniversalValue() : ParentVariant() {}

	template<typename UserValueT, typename CheckValidToType = typename CTypeToUniversalCType< UserValueT >::type>
	UniversalValue(UserValueT val) : ParentVariant(static_cast< typename CTypeToUniversalCType< UserValueT >::type >(val)) {} // Cast to nearest UniversalValue-supported type

	Datatype get_type() { return static_cast<Datatype>(this->which()); /* ASSUMPTION: constants in Type are 0-based and in the same order as the types supplied to the boost::variant superclass */ }
};

template<typename datatype_t>	struct UniversalValue::CTypeToUniversalCType<datatype_t, typename boost::enable_if< boost::is_unsigned< datatype_t > >::type >			{ typedef uint64_t type;	};
template<typename datatype_t>	struct UniversalValue::CTypeToUniversalCType<datatype_t, typename boost::enable_if< boost::is_signed< datatype_t > >::type >			{ typedef int64_t type;		};
template<typename datatype_t>	struct UniversalValue::CTypeToUniversalCType<datatype_t, typename boost::enable_if< boost::is_floating_point< datatype_t > >::type >	{ typedef double type;		};
template<> 						struct UniversalValue::CTypeToUniversalCType<std::string, void >																		{ typedef std::string type;	};
template<> 						struct UniversalValue::CTypeToUniversalCType<const char*, void >																		{ typedef std::string type;	};

#endif /* UNIVERSAL_VALUE_HPP_ */
