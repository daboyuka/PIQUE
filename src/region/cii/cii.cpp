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
 * cii.cpp
 *
 *  Created on: Feb 27, 2014
 *      Author: David A. Boyuka II
 */

#include <cmath>

#include <boost/smart_ptr.hpp>

#include "pique/region/cii/cii.hpp"
#include "pique/region/cii/cii-decoder.hpp"

constexpr RegionEncoding::Type CIIRegionEncoding::TYPE;
const size_t CIIRegionEncoding::CII_CHUNK_SIZE = pfor::PatchedFrameOfReference::kBatchSize;

size_t CIIRegionEncoding::get_size_in_bytes() const {
	return 	1 +                             // inverted
			is_compressed ?
				cii.size() * sizeof(char) : // cii, OR
				ii.size() * sizeof(rid_t);  // ii
}

void CIIRegionEncoding::dump() const {
	std::cout << "Domain size: " << this->domain_size << std::endl;
	std::cout << "Inverted flag? " << (this->is_inverted ? 'Y' : 'N') << std::endl;

	int mod = 0;
	ProgressiveCIIDecoder decoder(*this);

	while (decoder.has_top()) {
		std::cout << decoder.top() << " ";
		if ((++mod %= 16) == 0)
			std::cout << std::endl;

		decoder.next();
	}

	if (mod != 0)
		std::cout << std::endl;
}

uint64_t CIIRegionEncoding::get_element_count() const {
	if (is_compressed) {
		// TODO: Inefficient, maintain this count throughout CIIRegionEncoding lifetime, as there are times when it is already known
		// TODO: Another alternative/improvement: just check PFOR-Delta headers, skip chunk contents (requires code for deeper inspection of PFOR-Delta data)
		ProgressiveCIIDecoder decoder(*this);
		uint64_t count = 0;
		while (decoder.has_top()) {
			count++;
			decoder.next();
		}
		return count;
	} else {
		return this->is_inverted ? this->get_domain_size() - ii.size() : ii.size();
	}
}

void CIIRegionEncoding::convert_to_rids(std::vector<uint32_t> &out, bool sorted, bool preserve_self) {
	if (this->is_compressed || this->is_inverted) {
		ProgressiveCIIDecoder decoder(*this);

		out.clear();
		decoder.dump_to(out);
	} else {
		if (preserve_self)
			out = this->ii;
		else
			out = std::move(this->ii);
	}
}

void CIIRegionEncoding::compress() {
	if (this->is_compressed) {
		return;
	} else if (this->ii.empty()) {
		this->is_compressed = true;
		return;
	}

	const size_t compressed_chunks = (this->ii.size() - 1) - CIIRegionEncoding::CII_CHUNK_SIZE + 1;
	const size_t max_output_chunk_size = pfor::PatchedFrameOfReference::kSufficientBufferCapacity / sizeof(char);
	const size_t max_compressed_size = compressed_chunks * max_output_chunk_size;

	std::vector<rid_t> &ii_buffer = this->ii;
	std::vector<char> &cii_buffer = this->cii;
	cii_buffer.clear();
	cii_buffer.resize(max_compressed_size); // Resize with some new chars to hold the output chunk

	// size = bytes, length = element count
	char *output_chunk = &cii_buffer.front();
	rid_t *input_rids = &ii_buffer.front();
	size_t output_size = 0;
	size_t rids_left = ii_buffer.size();
	for (size_t chunk = 0; chunk < compressed_chunks; chunk++) {
		uint32_t input_chunk_length = std::min(rids_left, CIIRegionEncoding::CII_CHUNK_SIZE);
		uint32_t output_chunk_size;

		const bool success = pfor::PatchedFrameOfReference::encode(
				input_rids, CIIRegionEncoding::CII_CHUNK_SIZE, input_chunk_length,
				(void*)output_chunk, max_output_chunk_size, output_chunk_size);
		assert(success);

		output_chunk += output_chunk_size;
		output_size += output_chunk_size;
		input_rids += input_chunk_length;
		rids_left -= input_chunk_length;
	}

	cii_buffer.resize(output_size);
	ii_buffer.clear();
	this->is_compressed = true;
}

void CIIRegionEncoding::decompress() {
	if (!this->is_compressed)
		return;

	// BaseProgressiveCIIDecoder, since we don't want to invert it based on the inverted flag; we just want the actual RID contents
	BaseProgressiveCIIDecoder decoder(*this);

	decoder.dump_to(this->ii);
	this->cii.clear();
	this->is_compressed = false;
}

bool CIIRegionEncoding::operator==(const RegionEncoding &other_base) const {
	if (typeid(other_base) != typeid(CIIRegionEncoding))
		return false;
	const CIIRegionEncoding &other = dynamic_cast<const CIIRegionEncoding&>(other_base);

	if (this->is_compressed != other.is_compressed)
		return false;
	if (this->domain_size != other.domain_size)
		return false;

	// If both CIIs are inverted or not inverted, and both are compressed or not compressed,
	// we can directly compare their contents directly. Otherwise, we need to use a decoder
	// to normalize the contents.
	if (this->is_compressed == other.is_compressed && this->is_inverted == other.is_inverted) {
		if (this->is_compressed)
			return this->cii == other.cii;
		else
			return this->ii == other.ii;
	} else {
		ProgressiveCIIDecoder this_decoder(*this);
		ProgressiveCIIDecoder other_decoder(other);

		while (this_decoder.has_top() && other_decoder.has_top()) {
			if (this_decoder.top() != other_decoder.top())
				return false;
			this_decoder.next();
			other_decoder.next();
		}

		return !this_decoder.has_top() && !other_decoder.has_top();
	}
}
