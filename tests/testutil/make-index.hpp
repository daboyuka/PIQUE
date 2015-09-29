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
 * make-index-from-vector.hpp
 *
 *  Created on: Sep 8, 2014
 *      Author: David A. Boyuka II
 */
#ifndef MAKE_INDEX_FROM_VECTOR_HPP_
#define MAKE_INDEX_FROM_VECTOR_HPP_

#include <vector>
#include <boost/smart_ptr.hpp>

#include "pique/indexing/binning-spec.hpp"
#include "pique/indexing/binned-index.hpp"
#include "pique/indexing/index-builder.hpp"

template<typename RegionEncoderT, typename BinningSpecT, typename datatype_t>
static boost::shared_ptr< BinnedIndex >
make_index(typename RegionEncoderT::RegionEncoderConfig conf, boost::shared_ptr< BinningSpecT > binning_spec, const Dataset &dataset) {
	typedef typename RegionEncoderT::RegionEncodingOutT RegionEncodingT;

	IndexBuilder< datatype_t, RegionEncoderT, BinningSpecT > builder(conf, binning_spec);
	return builder.build_index(dataset);
}

template<typename RegionEncoderT, typename BinningSpecT, typename datatype_t>
static boost::shared_ptr< BinnedIndex >
make_index(typename RegionEncoderT::RegionEncoderConfig conf, boost::shared_ptr< BinningSpecT > binning_spec, boost::shared_ptr< const Dataset > dataset) {
	return make_index< RegionEncoderT, BinningSpecT, datatype_t >(conf, binning_spec, *dataset);
}

template<typename RegionEncoderT, typename datatype_t>
static boost::shared_ptr< BinnedIndex >
make_index(typename RegionEncoderT::RegionEncoderConfig conf, const Dataset &dataset) {
	using SigbitsBinningSpec = SigbitsBinningSpecification< datatype_t >;
	return make_index< RegionEncoderT, SigbitsBinningSpec, datatype_t >(conf, boost::make_shared< SigbitsBinningSpec >(sizeof(datatype_t)*8), dataset);
}

template<typename RegionEncoderT, typename datatype_t>
static boost::shared_ptr< BinnedIndex >
make_index(typename RegionEncoderT::RegionEncoderConfig conf, boost::shared_ptr< const Dataset > dataset) {
	return make_index< RegionEncoderT, datatype_t >(conf, *dataset);
}

template<typename RegionEncoderT, typename datatype_t>
static boost::shared_ptr< BinnedIndex >
make_index(typename RegionEncoderT::RegionEncoderConfig conf, const std::vector< datatype_t > &domain) {
	InMemoryDataset< datatype_t > dataset((std::vector< datatype_t >(domain)), Grid{domain.size()});
	return make_index< RegionEncoderT, datatype_t >(conf, dataset);
}

#endif /* MAKE_INDEX_FROM_VECTOR_HPP_ */
