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
 * cii-encode.cpp
 *
 *  Created on: Feb 28, 2014
 *      Author: David A. Boyuka II
 */

extern "C" {
#include <stdio.h>
#include <stdint.h>
}

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/cii/cii-encode.hpp"

#include <patchedframeofreference.h>

CIIRegionEncoder::CIIRegionEncoder(CIIRegionEncoderConfig conf, size_t total_elements) :
	RegionEncoder< CIIRegionEncoding, CIIRegionEncoderConfig >(conf, total_elements),
	next_rid(0),
	cur_chunk(),
	encoding(boost::make_shared< CIIRegionEncoding >(total_elements))
{
	encoding->is_compressed = true;
}

boost::shared_ptr< CIIRegionEncoding > CIIRegionEncoder::to_region_encoding() {
	return this->encoding;
}

void CIIRegionEncoder::push_bits_impl(uint64_t count, bool bitval) {
	// Only actually push 1-bits as RIDs; do nothing for 0-bits
	if (bitval) {
		uint64_t rids_remaining = count;

		for (rid_t rid = next_rid; rids_remaining; rid++, rids_remaining--) {
			this->cur_chunk.push_back(rid);
			if (this->cur_chunk.size() >= CIIRegionEncoding::CII_CHUNK_SIZE)
				this->compress_chunk();
		}
	}

	// Advance the current RID (bit) position in any case (this is the only
	// effect for 0-bit pushes)
	next_rid += count;
}

void CIIRegionEncoder::finalize_impl() {
	// When we are finished, compress and append the last residual chunk
	this->compress_chunk();
}

void CIIRegionEncoder::compress_chunk() {
	if (!cur_chunk.size())
		return;

	std::vector<char> &cii_buffer = this->encoding->cii;

	const size_t prev_cii_buffer_size = cii_buffer.size();
	const size_t max_output_chunk_size = pfor::PatchedFrameOfReference::kSufficientBufferCapacity / sizeof(char);
	cii_buffer.resize(prev_cii_buffer_size + max_output_chunk_size); // Resize with some new chars to hold the output chunk

	char *output_chunk = &(*cii_buffer.end()) - max_output_chunk_size;
	const rid_t *input_rids = &(*this->cur_chunk.begin());
	uint32_t output_chunk_size;

	const bool success = pfor::PatchedFrameOfReference::encode(
			input_rids, CIIRegionEncoding::CII_CHUNK_SIZE, this->cur_chunk.size(),
			(void*)output_chunk, max_output_chunk_size, output_chunk_size);
	assert(success);

	//printf("Compressed %lu RIDs into a new PFD chunk of size %lu\n", this->cur_chunk.size(), output_chunk_size);

	cii_buffer.resize(prev_cii_buffer_size + output_chunk_size);
	cur_chunk.clear();
}
