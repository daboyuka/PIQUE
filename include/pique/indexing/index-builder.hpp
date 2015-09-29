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
 * index-builder.hpp
 *
 *  Created on: Jan 24, 2014
 *      Author: David A. Boyuka II
 */

#ifndef _INDEX_BUILD_HPP
#define _INDEX_BUILD_HPP

#include <cstdint>
#include <cstdio>

#include <boost/smart_ptr.hpp>
#include <boost/mpl/bool.hpp>

#include "pique/data/dataset.hpp"
#include "pique/data/grid-subset.hpp"

#include "pique/indexing/binned-index.hpp"
#include "pique/indexing/quantization.hpp"

#include "pique/stats/stats.hpp"

template< typename RE >
struct IndexBuilderModeSmearOutOfBounds : public boost::mpl::false_ {};

template<int ndim>
class CBLQRegionEncoding;
template<int ndim>
struct IndexBuilderModeSmearOutOfBounds< CBLQRegionEncoding<ndim> > : public boost::mpl::true_ {};

struct IndexBuilderStats : public BaseStats< IndexBuilderStats > {
	template<typename CombineFn> void combine(const IndexBuilderStats &other, CombineFn combine) {
		combine(this->totaltime, other.totaltime);
		combine(this->indexingtime, other.indexingtime);
		combine(this->iostats, other.iostats);
		combine(this->num_bins, other.num_bins);
		combine(this->num_read_buffer_blocks, other.num_read_buffer_blocks);
	}

	TimeStats totaltime{};
	TimeStats indexingtime{};
	IOStats iostats{};
	uint64_t num_bins{0};
	uint64_t num_read_buffer_blocks{0};
};

DEFINE_STATS_SERIALIZE(IndexBuilderStats, stats, \
	stats.totaltime & stats.indexingtime & \
	stats.iostats & stats.num_bins & stats.num_read_buffer_blocks);

// Declarations
template<typename datatype_t, typename RegionEncoderT, typename BinningSpecificationT>
class IndexBuilder {
public:
	using RegionEncoderType = RegionEncoderT;
	using RegionEncoderConfigType = typename RegionEncoderType::RegionEncoderConfig;
	using BinningSpecificationType = BinningSpecificationT;
	using RegionEncodingType = typename RegionEncoderT::RegionEncodingOutT;
	using QuantizationType = typename BinningSpecificationT::QuantizationType;
	using QuantizedKeyCompareType = typename BinningSpecificationT::QuantizedKeyCompareType;

	IndexBuilder(RegionEncoderConfigType conf, boost::shared_ptr< BinningSpecificationType > binning_spec) :
		encoder_conf(std::move(conf)), binning_spec(binning_spec)
	{}

    boost::shared_ptr< BinnedIndex > build_index(const Dataset &data);
    boost::shared_ptr< BinnedIndex > build_index(const Dataset &data, GridSubset subset);

    IndexBuilderStats get_stats() const { return stats; }
    void reset_stats() { stats = IndexBuilderStats(); }

private:
    const RegionEncoderConfigType encoder_conf;
    boost::shared_ptr< BinningSpecificationType > binning_spec;

    mutable IndexBuilderStats stats;
};

#include "pique/indexing/impl/index-builder-impl.hpp"

#endif /* _INDEX_BUILD_HPP */
