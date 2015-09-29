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
 * binning-spec-impl.hpp
 *
 *  Created on: Aug 18, 2014
 *      Author: David A. Boyuka II
 */
#ifndef BINNING_SPEC_IMPL_HPP_
#define BINNING_SPEC_IMPL_HPP_

#include "pique/util/fixed-archive.hpp" // Include this before export.hpp
#include <boost/serialization/export.hpp>
#include "pique/util/typeconv.hpp"

#include "pique/indexing/binning-spec.hpp"

template<typename ValueTypeT>
auto ExplicitBinsBinningSpecification< ValueTypeT >::get_negative_infinity() -> QKeyType { return std::numeric_limits<QKeyType>::lowest(); }
template<>
auto ExplicitBinsBinningSpecification< std::string >::get_negative_infinity() -> QKeyType { return std::string(""); }

/* explicit instantiation */
#define BINNING_SPEC_INSTANTIATE_DATATYPE(binningclass, dtid) \
	template class binningclass< typename Datatypes::IndexableDatatypeInfo< dtid >::type >;

// Instantiate the necessary functions for each datatype
#define FOREACH_DATATYPE_DO(dtid) \
	BINNING_SPEC_INSTANTIATE_DATATYPE(ExplicitBinsBinningSpecification, dtid);
FOREACH_DATATYPE
#undef FOREACH_DATATYPE_DO

/*
// Configure the declaration of BinningSpecifications
#define BINNING_SPEC_CONFIGURE_DATATYPE(T) BINNING_SPEC_IMPLEMENT_DATATYPE(T)
BINNING_SPEC_CONFIGURE_NONSTRING_DATATYPES(SigbitsBinningSpecification)
BINNING_SPEC_CONFIGURE_ALL_DATATYPES(ExplicitBinsBinningSpecification)
#undef BINNING_SPEC_CONFIGURE_DATATYPE
*/

#endif /* BINNING_SPEC_IMPL_HPP_ */
