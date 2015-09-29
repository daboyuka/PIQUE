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
 * datatypes.hpp
 *
 *  Created on: Jun 4, 2014
 *      Author: David A. Boyuka II
 */
#ifndef DATATYPES_HPP_
#define DATATYPES_HPP_

#include <string>
#include <typeindex>

#include <boost/type_traits.hpp>
#include <boost/optional.hpp>
#include <boost/mpl/or.hpp>

namespace Datatypes {

enum struct IndexableDatatypeID : char {
	UNDEFINED = -1,
	BEGIN = 0,
	FLOAT = BEGIN,
	DOUBLE,
	UINT_8,
	SINT_8,
	UINT_16,
	SINT_16,
	UINT_32,
	SINT_32,
	UINT_64,
	SINT_64,
	STRING,
	END
};

enum struct NumericSignednessType {
	NOT_APPLICABLE,
	ONES_COMPLEMENT,
	TWOS_COMPLEMENT,
	UNSIGNED
};

// NumericSignednessTypeInfo specialization
template<typename ValueT, typename Enable = void >
struct NumericSignednessTypeInfo { static constexpr NumericSignednessType SIGNEDNESS_TYPE = NumericSignednessType::NOT_APPLICABLE; };

template<typename DatatypeT> struct is_datatype_numeric		: public boost::mpl::or_< boost::is_integral< DatatypeT >, boost::is_floating_point< DatatypeT > > {};
template<typename DatatypeT> struct is_datatype_unsigned	: public boost::is_unsigned			< DatatypeT > {};
template<typename DatatypeT> struct is_datatype_signed_1c	: public boost::is_floating_point	< DatatypeT > {};
template<typename DatatypeT> struct is_datatype_signed_2c	: public boost::is_signed			< DatatypeT > {};

template<typename DatatypeTypeT> struct CTypeToDatatypeID { static constexpr IndexableDatatypeID value = IndexableDatatypeID::UNDEFINED; };

template<IndexableDatatypeID DatatypeID> struct IndexableDatatypeInfo {
	static constexpr bool is_indexable_datatype = false;
	// typedef ... type;
	// static constexpr const char *ALIASES[] = {...};
};

struct DatatypeIDToCTypeDispatch; // Use like ValueToTypeDispatch

boost::optional<std::type_index> get_typeindex_by_name(std::string name);
boost::optional<std::type_index> get_typeindex_by_datatypeid(IndexableDatatypeID datatypeid);

boost::optional<std::string> get_name_by_typeindex(std::type_index typeindex);
boost::optional<std::string> get_name_by_datatypeid(IndexableDatatypeID datatypeid);

boost::optional<IndexableDatatypeID> get_datatypeid_by_typeindex(std::type_index typeindex);
boost::optional<IndexableDatatypeID> get_datatypeid_by_name(std::string name);

boost::optional<std::string> get_canonical_name(std::string alias);
boost::optional<NumericSignednessType> get_signedness(std::type_index typeindex);

};

#define FOREACH_DATATYPE_DO_HELPER(dtidsuf) FOREACH_DATATYPE_DO(Datatypes::IndexableDatatypeID::dtidsuf)
#define FOREACH_NONSTRING_DATATYPE \
		FOREACH_DATATYPE_DO_HELPER(FLOAT) \
		FOREACH_DATATYPE_DO_HELPER(DOUBLE) \
		FOREACH_DATATYPE_DO_HELPER(UINT_8) \
		FOREACH_DATATYPE_DO_HELPER(SINT_8) \
		FOREACH_DATATYPE_DO_HELPER(UINT_16) \
		FOREACH_DATATYPE_DO_HELPER(SINT_16) \
		FOREACH_DATATYPE_DO_HELPER(UINT_32) \
		FOREACH_DATATYPE_DO_HELPER(SINT_32) \
		FOREACH_DATATYPE_DO_HELPER(UINT_64) \
		FOREACH_DATATYPE_DO_HELPER(SINT_64)

#define FOREACH_DATATYPE \
		FOREACH_NONSTRING_DATATYPE \
		FOREACH_DATATYPE_DO_HELPER(STRING)

#include "impl/datatypes-impl.hpp"

#endif /* DATATYPES_HPP_ */
