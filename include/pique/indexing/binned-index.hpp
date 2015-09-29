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
 * binned-index.hpp
 *
 *  Created on: Jan 24, 2014
 *      Author: David A. Boyuka II
 */

#pragma once

#include <typeindex>
#include <vector>
#include <algorithm>
#include <iostream>
#include <boost/smart_ptr.hpp>
#include <boost/optional.hpp>

#include "pique/indexing/binned-index-types.hpp"
#include "pique/indexing/quantization.hpp"
#include "pique/indexing/binning-spec.hpp"
#include "pique/region/region-encoding.hpp"
#include "pique/encoding/index-encoding.hpp"

class BinnedIndex {
public:
	using bin_id_t = BinnedIndexTypes::bin_id_t;
	using bin_count_t = BinnedIndexTypes::bin_count_t;
	using region_id_t = BinnedIndexTypes::region_id_t;
	using region_count_t = BinnedIndexTypes::region_count_t;
	using bin_size_t = BinnedIndexTypes::bin_size_t;

public:
    BinnedIndex(
    		std::type_index indexed_datatype,
    		uint64_t domain_size,
    		boost::shared_ptr< const IndexEncoding > index_enc,
    		RegionEncoding::Type index_rep_type,
    		boost::shared_ptr< const AbstractBinningSpecification > bins,
    		std::vector< boost::shared_ptr< RegionEncoding > > regions) :
        indexed_datatype(indexed_datatype),
        domain_size(domain_size),
        index_enc(index_enc),
        index_rep_type(index_rep_type),
        bins(bins),
        regions(std::move(regions))
	{}
    ~BinnedIndex() {}

    size_t get_size_in_bytes() const {
    	size_t total_bytes = 0;
    	for (region_id_t i = 0; i < get_num_regions(); i++) {
        	const boost::shared_ptr< const RegionEncoding > region = get_region(i);
    		total_bytes += region->get_size_in_bytes();
    	}
    	return total_bytes;
    }

    // Metadata access
    std::type_index get_indexed_datatype() const { return indexed_datatype; }
    uint64_t get_domain_size() const { return domain_size; }
    boost::shared_ptr< const IndexEncoding > get_encoding() const { return index_enc; }
    RegionEncoding::Type get_representation_type() const { return index_rep_type; }

    // Bins access
    boost::shared_ptr< const AbstractBinningSpecification > get_binning_specification() const { return bins; }

    bin_count_t get_num_bins() const { return bins->get_num_bins(); };
    UniversalValue get_bin_key(bin_id_t bin) const { return bins->get_bin_key(bin); }
    std::vector< UniversalValue > get_all_bin_keys() const { return bins->get_all_bin_keys(); }
    bin_id_t lower_bound_bin(UniversalValue key, bin_id_t start_from = 0) const { return bins->lower_bound_bin(key, start_from); }
    bin_id_t upper_bound_bin(UniversalValue key, bin_id_t start_from = 0) const { return bins->upper_bound_bin(key, start_from); }

    // Region access
    region_count_t get_num_regions() const { return regions.size(); }

    boost::shared_ptr< RegionEncoding > get_region(region_id_t region) const { return regions[region]; }
    void get_regions(std::vector< boost::shared_ptr< RegionEncoding > > &out_regions, region_id_t start, region_id_t end) const {
    	out_regions.clear();
    	out_regions.insert(out_regions.end(), regions.begin() + start, regions.begin() + end);
    }
    void get_regions(std::vector< boost::shared_ptr< RegionEncoding > > &out_regions) const {
    	this->get_regions(out_regions, 0, get_num_regions());
    }

    // Convenience dump functions
    void dump() {
    	dump_summary();
    	for (size_t i = 0; i < bins->get_num_bins(); i++) {
    		std::cout << "Bin " << i << ": " <<
    				     "key = " << bins->get_bin_key(i) << " " << std::endl;
    		std::cout << std::endl;
    	}
    }

    void dump_summary() {
    	std::cout << "Indexed datatype: " << get_indexed_datatype().name() << std::endl;
    	std::cout << "Num bins: " << get_num_bins() << std::endl;
    	std::cout << "Num regions: " << get_num_regions() << std::endl;

    	size_t total_bytes = 0;
    	for (region_id_t i = 0; i < get_num_regions(); i++)
    		total_bytes += get_region(i)->get_size_in_bytes();

    	std::cout << "Total region bytes: " << total_bytes << std::endl;
    }

    // Creates a new AbstractBinnedIndex that shares the same indexed datatype, index representation, and binning specification
    // as this index, but with a new index encoding (IndexEncodingType) and regions
    boost::shared_ptr< BinnedIndex > derive_new_index(boost::shared_ptr< const IndexEncoding > new_index_enc, std::vector< boost::shared_ptr< RegionEncoding > > new_regions) const {
    	return boost::make_shared< BinnedIndex >(
    			this->get_indexed_datatype(),
    			this->get_domain_size(),
    			new_index_enc,
    			this->get_representation_type(),
    			this->get_binning_specification(),
    			std::move(new_regions));
    }

private:
    const std::type_index indexed_datatype;
    const uint64_t domain_size;
    const boost::shared_ptr< const IndexEncoding > index_enc;
    const RegionEncoding::Type index_rep_type;

    const boost::shared_ptr< const AbstractBinningSpecification > bins;

    const std::vector< boost::shared_ptr< RegionEncoding > > regions;

    template<typename datatype_t, typename RegionEncoderT, typename BinningSpecificationT> friend class IndexBuilder;
    friend class IndexAccumulator;
};
