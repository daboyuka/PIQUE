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
 * region-encoding.cpp
 *
 *  Created on: May 6, 2014
 *      Author: David A. Boyuka II
 */

#include <typeindex>

#include <boost/make_shared.hpp>

#include <boost/bimap.hpp>
#include <boost/bimap/unordered_set_of.hpp>
#include <boost/optional.hpp>
#include <boost/none.hpp>

#include "pique/util/dynamic-dispatch.hpp"

#include "pique/region/region-encoding.hpp"
#include "pique/region/ii/ii.hpp"
#include "pique/region/cii/cii.hpp"
#include "pique/region/cblq/cblq.hpp"
#include "pique/region/wah/wah.hpp"
#include "pique/region/bitmap/bitmap.hpp"

using RETypeToClassDispatch =
		typename MakeValueToTypeDispatch<RegionEncoding::Type>
	::WithValues< RegionEncoding::Type::II, RegionEncoding::Type::CII, RegionEncoding::Type::WAH, RegionEncoding::Type::CBLQ_2D, RegionEncoding::Type::CBLQ_3D, RegionEncoding::Type::CBLQ_4D, RegionEncoding::Type::UNCOMPRESSED_BITMAP >
	::WithTypes< IIRegionEncoding, CIIRegionEncoding, WAHRegionEncoding, CBLQRegionEncoding<2>, CBLQRegionEncoding<3>, CBLQRegionEncoding<4>, BitmapRegionEncoding >::type;

boost::shared_ptr< RegionEncoding > RegionEncoding::make_null_region(RegionEncoding::Type type) {
	return RETypeToClassDispatch::dispatchMatching< boost::shared_ptr< RegionEncoding > >(
			type,
			boost::make_shared_dispatch< RegionEncoding >(),
			nullptr);
}

boost::shared_ptr< RegionEncoding > RegionEncoding::make_uniform_region(RegionEncoding::Type type, uint64_t nelem, bool filled) {
	return RETypeToClassDispatch::dispatchMatching< boost::shared_ptr< RegionEncoding > >(
			type,
			boost::make_shared_dispatch< RegionEncoding >(),
			nullptr,
			nelem, filled);
}

typedef boost::bimaps::bimap<
			boost::bimaps::unordered_set_of< std::string, std::hash< std::string > >,
			boost::bimaps::unordered_set_of< RegionEncoding::Type > >
		RepTypeMap;

static const std::vector< typename RepTypeMap::value_type > known_reptypes_initlist = {
		{"ii", RegionEncoding::Type::II},
		{"cii", RegionEncoding::Type::CII},
		{"wah", RegionEncoding::Type::WAH},
		{"cblq2d", RegionEncoding::Type::CBLQ_2D},
		{"cblq3d", RegionEncoding::Type::CBLQ_3D},
		{"cblq4d", RegionEncoding::Type::CBLQ_4D},
		{"bitmap", RegionEncoding::Type::UNCOMPRESSED_BITMAP},
};

static const RepTypeMap known_reptypes(known_reptypes_initlist.begin(), known_reptypes_initlist.end());

typedef boost::bimaps::bimap<
			boost::bimaps::unordered_set_of< RegionEncoding::Type >,
			boost::bimaps::unordered_set_of< std::type_index, std::hash< std::type_index > > >
		ClassTypeMap;

static const std::vector< typename ClassTypeMap::value_type > known_classtypes_initlist = {
		{RegionEncoding::Type::II, typeid(IIRegionEncoding)},
		{RegionEncoding::Type::CII, typeid(CIIRegionEncoding)},
		{RegionEncoding::Type::WAH, typeid(WAHRegionEncoding)},
		{RegionEncoding::Type::CBLQ_2D, typeid(CBLQRegionEncoding<2>)},
		{RegionEncoding::Type::CBLQ_3D, typeid(CBLQRegionEncoding<3>)},
		{RegionEncoding::Type::CBLQ_4D, typeid(CBLQRegionEncoding<4>)},
		{RegionEncoding::Type::UNCOMPRESSED_BITMAP, typeid(BitmapRegionEncoding)},
};

static const ClassTypeMap known_classtypes(known_classtypes_initlist.begin(), known_classtypes_initlist.end());

boost::optional< std::type_index > RegionEncoding::get_region_representation_class_by_type(RegionEncoding::Type type) {
	auto it = known_classtypes.left.find(type);
	return (it != known_classtypes.left.end() ?
				boost::optional< std::type_index >(it->second) :
				boost::none);
}

boost::optional< RegionEncoding::Type > RegionEncoding::get_region_representation_type_by_name(std::string name) {
	auto it = known_reptypes.left.find(name);
	return (it != known_reptypes.left.end() ?
				boost::optional< RegionEncoding::Type >(it->second) :
				boost::none);
}

boost::optional< std::string > RegionEncoding::get_region_representation_type_name(RegionEncoding::Type reptype) {
	auto it = known_reptypes.right.find(reptype);
	return (it != known_reptypes.right.end() ?
				boost::optional< std::string >(it->second) :
				boost::none);
}

void RegionEncoding::convert_to_rids(std::vector<uint64_t>& out, uint64_t offset, bool sorted, bool preserve_self) {
	std::vector< uint32_t > rids;
	std::vector< uint64_t > rids64;

	this->convert_to_rids(rids, sorted, preserve_self);
	rids64.resize(rids.size());

	auto in_it = rids.cbegin(), in_end_it = rids.cend();
	auto out_it = rids64.begin();
	while (in_it != in_end_it) {
		*out_it = *in_it + offset;
		++in_it; ++out_it;
	}
}
