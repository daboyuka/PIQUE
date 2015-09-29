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
 * datatypes.cpp
 *
 *  Created on: Jun 3, 2014
 *      Author: David A. Boyuka II
 */

#include <string>
#include <typeindex>
#include <unordered_map>

#include <boost/type_traits.hpp>
#include <boost/optional.hpp>
#include <boost/none.hpp>
#include <boost/bimap.hpp>
#include <boost/bimap/unordered_set_of.hpp>

#include "pique/util/datatypes.hpp"

namespace Datatypes {

// Allocate space for all datatype alias arrays

// For non-IndexableDatatypeID types
template<typename DatatypeTypeT>
constexpr IndexableDatatypeID CTypeToDatatypeID< DatatypeTypeT >::value;

// For IndexableDatatypeID types
#undef FOREACH_DATATYPE_DO
#define FOREACH_DATATYPE_DO(dtid) \
	constexpr const char * IndexableDatatypeInfo< dtid >::ALIASES[]; \
	constexpr IndexableDatatypeID CTypeToDatatypeID< typename IndexableDatatypeInfo< dtid >::type >::value;
FOREACH_DATATYPE
#undef FOREACH_DATATYPE_DO

struct DatatypeInfo {
private:
	static constexpr int MAX_ALIASES = 64;
public:
	DatatypeInfo(IndexableDatatypeID dtid, std::type_index ti, std::vector< std::string > names, NumericSignednessType signedness) :
		dtid(dtid), ti(ti), names(names), signedness(signedness)
	{}
	DatatypeInfo(IndexableDatatypeID dtid, std::type_index ti, const char * const * names, NumericSignednessType signedness) :
		dtid(dtid), ti(ti), names(names, std::find(names, names + MAX_ALIASES, nullptr)), signedness(signedness)
	{}

	const IndexableDatatypeID dtid;
	const std::type_index ti;
	const std::vector< std::string > names;
	const NumericSignednessType signedness;
};

struct DatatypeRegistry {
public:
	typedef boost::bimaps::unordered_set_of< std::string, std::hash< std::string > > StringKeyType;
	typedef boost::bimaps::unordered_set_of< std::type_index, std::hash< std::type_index > > TypeindexKeyType;
	typedef boost::bimaps::unordered_set_of< IndexableDatatypeID > DatatypeIDKeyType;

	typedef boost::bimaps::bimap< StringKeyType, TypeindexKeyType > NameToTypeindexBimap;
	typedef boost::bimaps::bimap< StringKeyType, DatatypeIDKeyType > NameToDatatypeIDBimap;
	typedef boost::bimaps::bimap< TypeindexKeyType, DatatypeIDKeyType > TypeindexToDatatypeIDBimap;

public:
	DatatypeRegistry(std::initializer_list< DatatypeInfo > datatypes) {
		init(std::move(datatypes));
	}

	DatatypeRegistry() {
		init(append_datatype_info< IndexableDatatypeID::BEGIN >(std::vector< DatatypeInfo >()));
	}

private:
	template<IndexableDatatypeID Datatype>
	std::vector< DatatypeInfo > append_datatype_info(std::vector< DatatypeInfo > dtinfos);

	template<typename DatatypeInfoList>
	void init(DatatypeInfoList datatypes) {
		for (auto dt_it = datatypes.begin(); dt_it != datatypes.end(); dt_it++) {
			const DatatypeInfo &datatype = *dt_it;
			const std::string &canonical_name = datatype.names.front();

			name_to_typeindex.left.insert(std::make_pair(canonical_name, datatype.ti));
			name_to_datatypeid.left.insert(std::make_pair(canonical_name, datatype.dtid));
			typeindex_to_datatypeid.left.insert(std::make_pair(datatype.ti, datatype.dtid));

			signednesses.insert(std::make_pair(datatype.ti, datatype.signedness));

			for (auto name_it = datatype.names.begin(); name_it != datatype.names.end(); name_it++) {
				const std::string &name = *name_it;
				name_aliases.insert(std::make_pair(name, canonical_name));
			}
		}
	}

public:
	NameToTypeindexBimap name_to_typeindex;
	NameToDatatypeIDBimap name_to_datatypeid;
	TypeindexToDatatypeIDBimap typeindex_to_datatypeid;

	std::unordered_map<std::string, std::string> name_aliases;
	std::unordered_map<std::type_index, NumericSignednessType> signednesses;
};

template<IndexableDatatypeID DTID>
std::vector< DatatypeInfo > DatatypeRegistry::append_datatype_info(std::vector< DatatypeInfo > dtinfos) {
	using DatatypeType = typename IndexableDatatypeInfo<DTID>::type;
	dtinfos.push_back(DatatypeInfo(
		DTID,
		typeid(DatatypeType),
		IndexableDatatypeInfo< DTID >::ALIASES,
		NumericSignednessTypeInfo< DatatypeType >::SIGNEDNESS_TYPE
	));
	return append_datatype_info< (IndexableDatatypeID)((int)DTID + 1) >(dtinfos);
}
template<>
std::vector< DatatypeInfo > DatatypeRegistry::append_datatype_info<IndexableDatatypeID::END>(std::vector< DatatypeInfo > dtinfos) {
	return dtinfos;
}

DatatypeRegistry datatypes;

template<typename RightT, typename MapT>
boost::optional< RightT > retrieve_associated_value(const MapT &map, const typename MapT::key_type &left) {
	auto it = map.find(left);
	if (it != map.end()) {
		const typename MapT::mapped_type &val = it->second;
		return boost::optional< RightT >(val);
	} else {
		return boost::none;
	}
}

boost::optional<std::string> get_canonical_name(std::string alias) {
	return retrieve_associated_value<std::string>(datatypes.name_aliases, alias);
}

boost::optional<std::type_index> get_typeindex_by_name(std::string name) {
	boost::optional<std::string> canonical_name = get_canonical_name(name);
	if (!canonical_name) return boost::none;
	return retrieve_associated_value<std::type_index>(datatypes.name_to_typeindex.left, *canonical_name);
}

boost::optional<IndexableDatatypeID> get_datatypeid_by_name(std::string name) {
	boost::optional<std::string> canonical_name = get_canonical_name(name);
	if (!canonical_name) return boost::none;
	return retrieve_associated_value<IndexableDatatypeID>(datatypes.name_to_datatypeid.left, *canonical_name);
}

boost::optional<IndexableDatatypeID> get_datatypeid_by_typeindex(std::type_index typeindex) {
	return retrieve_associated_value<IndexableDatatypeID>(datatypes.typeindex_to_datatypeid.left, typeindex);
}

boost::optional<std::string> get_name_by_typeindex(std::type_index typeindex) {
	return retrieve_associated_value<std::string>(datatypes.name_to_typeindex.right, typeindex);
}

boost::optional<std::type_index> get_typeindex_by_datatypeid(IndexableDatatypeID datatypeid) {
	return retrieve_associated_value<std::type_index>(datatypes.typeindex_to_datatypeid.right, datatypeid);
}

boost::optional<std::string> get_name_by_datatypeid(IndexableDatatypeID datatypeid) {
	return retrieve_associated_value<std::string>(datatypes.name_to_datatypeid.right, datatypeid);
}

boost::optional<NumericSignednessType> get_signedness(std::type_index typeindex) {
	return retrieve_associated_value<NumericSignednessType>(datatypes.signednesses, typeindex);
}

};
