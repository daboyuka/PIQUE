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
 * datatypes-impl.hpp
 *
 *  Created on: Aug 19, 2014
 *      Author: David A. Boyuka II
 */
#ifndef DATATYPES_IMPL_HPP_
#define DATATYPES_IMPL_HPP_

#include "pique/util/datatypes.hpp"
#include "pique/util/dynamic-dispatch.hpp"

namespace Datatypes {

// NumericSignednessTypeInfo specialization
template<typename ValueT>
struct NumericSignednessTypeInfo< ValueT, typename boost::enable_if< boost::is_signed< ValueT > >::type > { static constexpr NumericSignednessType SIGNEDNESS_TYPE = NumericSignednessType::TWOS_COMPLEMENT; };

template<typename ValueT>
struct NumericSignednessTypeInfo< ValueT, typename boost::enable_if< boost::is_unsigned< ValueT > >::type> { static constexpr NumericSignednessType SIGNEDNESS_TYPE = NumericSignednessType::UNSIGNED; };

template<typename ValueT>
struct NumericSignednessTypeInfo< ValueT, typename boost::enable_if< boost::is_floating_point< ValueT > >::type> { static constexpr NumericSignednessType SIGNEDNESS_TYPE = NumericSignednessType::ONES_COMPLEMENT; };

// Definition of all datatypes
#define DEFINE_DATATYPE(dtid, dttype, ...) \
	template<> struct IndexableDatatypeInfo<Datatypes::IndexableDatatypeID::dtid> {\
		static constexpr bool is_indexable_datatype = true; \
		typedef dttype type; \
		static constexpr const char *ALIASES[] = { __VA_ARGS__, nullptr }; \
	}; \
	template<> struct CTypeToDatatypeID<dttype> { static constexpr IndexableDatatypeID value = IndexableDatatypeID::dtid; };

DEFINE_DATATYPE(FLOAT,		float, 			"float32", "float");
DEFINE_DATATYPE(DOUBLE,		double, 		"float64", "double");
DEFINE_DATATYPE(UINT_8,		uint8_t,		"uint8", "uchar");
DEFINE_DATATYPE(SINT_8,		int8_t,			"int8", "char");
DEFINE_DATATYPE(UINT_16,	uint16_t,		"uint16", "ushort");
DEFINE_DATATYPE(SINT_16,	int16_t,		"int16", "short");
DEFINE_DATATYPE(UINT_32,	uint32_t,		"uint32", "uint");
DEFINE_DATATYPE(SINT_32,	int32_t,		"int32", "int");
DEFINE_DATATYPE(UINT_64,	uint64_t,		"uint64", "ulong");
DEFINE_DATATYPE(SINT_64,	int64_t,		"int64", "long");
DEFINE_DATATYPE(STRING,		std::string,	"string", "char*");

template<IndexableDatatypeID DTID, typename... DTIDToCTypes>
struct DatatypeIDToCTypeDispatchHelper :
		public DatatypeIDToCTypeDispatchHelper<
			(IndexableDatatypeID)((int)DTID + 1),
			ValueToType< IndexableDatatypeID, DTID, typename IndexableDatatypeInfo< DTID >::type >,
			DTIDToCTypes...> {};
template<typename... DTIDToCTypes>
struct DatatypeIDToCTypeDispatchHelper<IndexableDatatypeID::END, DTIDToCTypes...> :
		public ValueToTypeDispatch< DTIDToCTypes... > {};

template<IndexableDatatypeID CurDTID, IndexableDatatypeID... PrevDTIDs>
struct DatatypeIDDispatchHelper : public DatatypeIDDispatchHelper< (IndexableDatatypeID)((int)CurDTID + 1), CurDTID, PrevDTIDs...> {};
template<IndexableDatatypeID... AllDTIDs>
struct DatatypeIDDispatchHelper<IndexableDatatypeID::END, AllDTIDs...> : public ValueDispatch< IndexableDatatypeID, AllDTIDs... > {};

struct DatatypeIDToCTypeDispatch : public DatatypeIDToCTypeDispatchHelper< IndexableDatatypeID::BEGIN > {};
struct DatatypeIDDispatch : public DatatypeIDDispatchHelper< IndexableDatatypeID::BEGIN > {};

};


#endif /* DATATYPES_IMPL_HPP_ */
