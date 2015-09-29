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
 * index-build-impl.hpp
 *
 *  Created on: Jan 28, 2014
 *      Author: drew
 */

#ifndef _INDEX_BUILD_IMPL_HPP
#define _INDEX_BUILD_IMPL_HPP

#include <iostream>
#include <cassert>
#include <vector>
#include <boost/unordered_map.hpp>
#include <boost/make_shared.hpp>

//#include "pique/util/zo-iter2.hpp"
#include "pique/util/datatypes.hpp"

#include "pique/data/dataset.hpp"
#include "pique/indexing/binned-index.hpp"
#include "pique/region/region-encoding.hpp"
#include "pique/encoding/index-encoding.hpp"

// Template definitions and specializations

template<typename datatype_t, typename RegionEncoderT, typename BinningSpecificationT>
boost::shared_ptr< BinnedIndex >
IndexBuilder< datatype_t, RegionEncoderT, BinningSpecificationT >::build_index(const Dataset &data) {
	return this->build_index(data, GridSubset(data.get_grid()));
}

template<typename datatype_t>
static boost::shared_ptr< BufferedDatasetStream< datatype_t > > open_buffered_dataset_stream(const Dataset &data, GridSubset subset) {
	static constexpr const Datatypes::IndexableDatatypeID DTID = Datatypes::CTypeToDatatypeID< datatype_t >::value;
	using TypedDatasetStream = DatasetStream< datatype_t >;
    using TypedBufferedDatasetStream = BufferedDatasetStream< datatype_t >;

    boost::shared_ptr< AbstractDatasetStream > abstract_datastream = data.open_stream(std::move(subset));
    assert(abstract_datastream->get_datatype() == DTID);

    // Cast it to the datatype of this index build (check validity of cast)
    boost::shared_ptr< TypedDatasetStream > datastream = boost::dynamic_pointer_cast< TypedDatasetStream >(abstract_datastream);
    assert(datastream);

    // Get a buffered dataset stream (use the current one if it's already buffered, otherwise wrap it in a buffer); this is to amortize virtual function call cost
    boost::shared_ptr< TypedBufferedDatasetStream > buffered_datastream = boost::dynamic_pointer_cast< TypedBufferedDatasetStream >(datastream);
    if (!buffered_datastream)
    	buffered_datastream = boost::make_shared< BlockBufferingDatasetStream< datatype_t > >(datastream);

    return buffered_datastream;
}

template<typename datatype_t, typename RegionEncoderT, typename BinningSpecificationT>
boost::shared_ptr< BinnedIndex >
IndexBuilder< datatype_t, RegionEncoderT, BinningSpecificationT >::build_index(const Dataset &data, GridSubset subset) {
    using IndexableDatatypeID = Datatypes::IndexableDatatypeID;
	//static constexpr const IndexableDatatypeID DTID = Datatypes::CTypeToDatatypeID< datatype_t >::value;

    using bin_id_t = BinnedIndexTypes::bin_id_t;
    using bin_count_t = BinnedIndexTypes::bin_count_t;
    using bin_size_t = BinnedIndexTypes::bin_size_t;
    using region_id_t = BinnedIndexTypes::region_id_t;
    using region_count_t = BinnedIndexTypes::region_count_t;

    using bin_qkey_t = typename BinningSpecificationType::QKeyType;
    using bin_qkey_to_encoder_map_t = boost::unordered_map< bin_qkey_t, RegionEncoderType >; // Because for a flat index, bin ID == region ID

    using TypedDatasetStream = DatasetStream< datatype_t >;
    using TypedBufferedDatasetStream = BufferedDatasetStream< datatype_t >;

    TIME_STATS_TIME_BEGIN(stats.totaltime) // Time everything

    // Builder data structures
    bin_qkey_to_encoder_map_t encoders_by_qkey;
    boost::shared_ptr< BinningSpecificationType > binning_spec_dup = boost::make_shared< BinningSpecificationType >(*this->binning_spec);
    const QuantizationType &quant = binning_spec_dup->get_quantization();
    const QuantizedKeyCompareType &qcompare = binning_spec_dup->get_quantized_key_compare();

    // Open a dataset stream
    boost::shared_ptr< TypedBufferedDatasetStream > buffered_datastream = open_buffered_dataset_stream< datatype_t >(data, std::move(subset));

    // Extract constants for ease of use
    const uint64_t nelem = buffered_datastream->get_element_count();

    // Iterate over all runs of bin-equal values in the dataset
    uint64_t value_pos = 0;
    while (buffered_datastream->has_next()) {
    	using buffer_iterator_t = typename TypedBufferedDatasetStream::buffer_iterator_t;
    	buffer_iterator_t data_it, data_end_it;

    	buffered_datastream->get_buffered_data(data_it, data_end_it);
    	++stats.num_read_buffer_blocks;

        TIME_STATS_TIME_BEGIN(stats.indexingtime)
    	while (data_it != data_end_it) {
    		// Retrieve the current value, which will begin a new run
    		const datatype_t run_val = *data_it;
    		const uint64_t run_start_pos = value_pos;

    		// Move past this element in preparation to continue scanning
    		++data_it;
    		++value_pos;

    		// Compute the corresponding bin header for this run value
    		const bin_qkey_t run_qkey = quant.quantize(run_val);

    		auto encoder_for_qkey_it = encoders_by_qkey.find(run_qkey);

    		// If this is a new bin, also allocate a new bin builder
    		if (encoder_for_qkey_it == encoders_by_qkey.end())
    			encoder_for_qkey_it = encoders_by_qkey.emplace(std::make_pair(run_qkey, RegionEncoderType(this->encoder_conf, nelem))).first;

    		// Move it to the first value != run_val (or the end of the array)
    		while (data_it != data_end_it) {
    			const datatype_t cur_val = *data_it;
    			const bin_qkey_t cur_qkey = quant.quantize(cur_val);

    			if (cur_qkey != run_qkey)
    				break;

    			++data_it;
    			++value_pos;
    		}

    		// Compute the run length, then append bits to the run's bin's region encoder
    		const uint64_t run_length = value_pos - run_start_pos;
    		encoder_for_qkey_it->second.insert_bits(run_start_pos, run_length);
    	}
        TIME_STATS_TIME_END()
    }

    stats.iostats += buffered_datastream->get_stats();
    stats.num_bins += encoders_by_qkey.size();

    std::vector< boost::shared_ptr< RegionEncoding > > sorted_regions;

    TIME_STATS_TIME_BEGIN(stats.indexingtime)
    // Construct a vector of all qkeys seen
    std::vector< bin_qkey_t > sorted_bin_qkeys;
    sorted_bin_qkeys.reserve(encoders_by_qkey.size());
    for (auto it = encoders_by_qkey.begin(); it != encoders_by_qkey.end(); ++it)
    	sorted_bin_qkeys.push_back(it->first);

    // Sort qkeys to form the final bin list
    std::sort(sorted_bin_qkeys.begin(), sorted_bin_qkeys.end(), [&qcompare](const bin_qkey_t &k1, const bin_qkey_t &k2)->bool { return qcompare.compare(k1, k2); });

    // Load the sorted qkeys into the binning specification as the final bin list
    binning_spec_dup->populate(sorted_bin_qkeys);

    // Collect the region encodings in sorted-bin order
    for (bin_qkey_t bin_qkey : sorted_bin_qkeys) {
    	RegionEncoderType &encoder = encoders_by_qkey.find(bin_qkey)->second;
    	encoder.finalize();
    	boost::shared_ptr< RegionEncoding > bin_region = encoder.to_region_encoding();
    	sorted_regions.push_back(bin_region);
    }
    TIME_STATS_TIME_END()

    // Finally, compose the output index to be returned
    boost::shared_ptr< BinnedIndex > index_out =
    		boost::make_shared< BinnedIndex >(
    				typeid(datatype_t),
    				nelem,
    				IndexEncoding::get_equality_encoding_instance(),
    				RegionEncodingType::TYPE,
    				binning_spec_dup,
    				sorted_regions);

    return index_out;

    TIME_STATS_TIME_END()
}

/*
template<typename datatype_t, typename RegionEncoderT>
boost::shared_ptr< AbstractBinnedIndex >
IndexBuilder<datatype_t, RegionEncoderT>::build_index_zo(const Dataset<datatype_t> &data, int sigbits) {
    typedef typename AbstractBinnedIndex::bin_header_t bin_header_t;
    typedef BinnedIndexTypes::bin_count_t bin_count_t;
    typedef BinnedIndexTypes::bin_id_t bin_id_t;
    typedef BinnedIndexTypes::bin_size_t bin_size_t;

    typedef boost::unordered_map<bin_header_t, bin_id_t> bin_header_to_id_map_t;

    // Extract constants for ease of use
    const int insigbits = (sizeof(datatype_t) << 3) - sigbits;
    const int ndim = data.get_grid()->get_ndim();
    const uint64_t *dims = data.get_grid()->get_grid()->dims;
    const size_t nelem = data.get_grid()->get_npts();

    // Builder data structures

    // The bin header -> bin ID mapping table for use during construction
    bin_header_to_id_map_t bin_header_to_bin_id;
    // Bin index CBLQ builders
    std::vector<RegionEncoderT> bin_encoders;
    std::vector<bin_header_t> bin_headers;

    // Iterate over all runs of bin-equal values in the dataset
    const std::vector<datatype_t> &data_arr = *data.get_data();

    bool first_run = true;
    bin_header_t run_bin_header = (type_convert<datatype_t, bin_header_t>(data_arr[0]) >> insigbits) + 1; // != first bin header, will trigger the first run
    bin_id_t run_bin;
    uint64_t run_start_pos;
    uint64_t last_zid;

    ZOIterLoopBody loop = [&](uint64_t zid, uint64_t rmoid, uint64_t coords[]) {
        // Retrieve the current value, which will begin a new run
        const datatype_t cur_val = data_arr[rmoid];

        // Compute the corresponding bin header for this run value
        const bin_header_t cur_bin_header = type_convert<datatype_t, bin_header_t>(cur_val) >> insigbits;

		// If we changed bins OR skipped elements due to out-of-bounds...
		if (cur_bin_header != run_bin_header || zid != last_zid + 1) {
			// Insert the current run as a bit run (unless this is the first
			if (!first_run) {
				const bin_size_t run_length_smear = zid - run_start_pos;
				const bin_size_t run_length_nosmear = last_zid - run_start_pos + 1;

				const bin_size_t run_length = (IndexBuilderModeSmearOutOfBounds< RegionEncodingT >() == true) ? run_length_smear : run_length_nosmear;
				bin_encoders[run_bin].insert_bits(run_start_pos, run_length);
			} else {
				first_run = false;
			}

			// Start a new run
			run_bin_header = cur_bin_header;
        	run_start_pos = zid;

			// Find the bin ID for this bin header, allocating a new bin ID
			// if this is its first appearance
			const bin_id_t next_bin_id = bin_encoders.size();
			std::pair<typename bin_header_to_id_map_t::iterator, bool> bin_id_register =
					bin_header_to_bin_id.emplace(run_bin_header, next_bin_id);

			// If this is a new bin, also allocate a new bin builder
	        if (bin_id_register.second) {
	            bin_encoders.emplace_back(this->encoder_conf, nelem); // emplace = push new region encoder object constructed with an encoder conf and the total elements
	            bin_headers.push_back(run_bin_header);
	        }

	        run_bin = bin_id_register.first->second; // emplace_result.iterator->key(bin_id)
        }

		last_zid = zid;
    };

    zo_loop_iterate(ndim, dims, dims, true, loop);

    // The last run is always unfinished
    const bin_size_t last_run_length = last_zid - run_start_pos + 1;
	bin_encoders[run_bin].insert_bits(run_start_pos, last_run_length);

    // The output index to be returned
    boost::shared_ptr< AbstractBinnedIndex > index_out = boost::make_shared< AbstractBinnedIndex >(sigbits, typeid(datatype_t), AbstractBinnedIndex::BinCompositionType::FLAT);

    // All data has been partitioned into index bins. The bins are out of order, and are
    // also "unfinalized" (need trailing 0's appended).
    auto bin_encoder_it = bin_encoders.begin();
    auto bin_header_it = bin_headers.begin();
    while (bin_encoder_it != bin_encoders.end()) {
        // Finalize and retrieve the bin index
    	bin_encoder_it->finalize();
    	boost::shared_ptr< RegionEncoding > bin_index = bin_encoder_it->to_region_encoding();

    	// Append the bin index to the aggregate output index
    	index_out->append_bin(*bin_header_it, bin_index);

    	// Increment to the next bin
    	bin_header_it++;
    	bin_encoder_it++;
    }

    index_out->sort_bins();

    return index_out;
}
*/

#endif /* _INDEX_BUILD_IMPL_HPP */
