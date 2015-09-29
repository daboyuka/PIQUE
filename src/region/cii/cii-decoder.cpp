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
 * cii-decoder.cpp
 *
 *  Created on: Mar 22, 2014
 *      Author: David A. Boyuka II
 */

#include <vector>
#include <boost/iterator/counting_iterator.hpp>

#include <patchedframeofreference.h>

#include "pique/region/cii/cii.hpp"
#include "pique/region/cii/cii-decoder.hpp"

BaseProgressiveCIIDecoder::BaseProgressiveCIIDecoder(const CIIRegionEncoding &cii) :
	is_decompressing_cii(cii.is_compressed), cur_chunk()
{
	if (is_decompressing_cii) {
		cur_chunk_it = cur_chunk.cbegin();
		cur_chunk_it_end = cur_chunk.cend();
		encoded_chunk_it = cii.cii.cbegin();
		encoded_chunk_it_end = cii.cii.cend();
		decode_next_chunk();
	} else {
		cur_chunk_it = cii.ii.cbegin();
		cur_chunk_it_end = cii.ii.cend();
	}
}

bool BaseProgressiveCIIDecoder::has_top() const {
	return this->cur_chunk_it != this->cur_chunk_it_end;
}

BaseProgressiveCIIDecoder::rid_t BaseProgressiveCIIDecoder::top() const {
	return *cur_chunk_it;
}

void BaseProgressiveCIIDecoder::next() {
	cur_chunk_it++;
	if (cur_chunk_it == cur_chunk_it_end)
		this->decode_next_chunk();
}

void BaseProgressiveCIIDecoder::dump_to(std::vector<rid_t> &output) {
	// Copy any remaining decoded RIDs to the output buffer
	output.insert(output.end(), cur_chunk_it, cur_chunk_it_end);
	cur_chunk.clear();
	cur_chunk_it = cur_chunk.begin();
	cur_chunk_it_end = cur_chunk.end();

	// Then, decode any remaining CII chunks directly to the output buffer
	while (decode_next_chunk_to(output));
}

bool BaseProgressiveCIIDecoder::decode_next_chunk_to(std::vector<rid_t> &output) {
	if (!this->is_decompressing_cii || this->encoded_chunk_it == this->encoded_chunk_it_end)
		return false;

	// Prepare room in the vector for the decoded chunk
	size_t old_elemcount = output.size();
	output.resize(old_elemcount + CIIRegionEncoding::CII_CHUNK_SIZE);

	// Set up input/output parameters for chunk decoding
	const char *encoded_chunk_in = &*encoded_chunk_it;
	size_t remaining_encoded_length = encoded_chunk_it_end - encoded_chunk_it;
	uint32_t encoded_chunk_length; // To be filled

	rid_t *decoded_chunk_out = &output[old_elemcount];
	uint32_t unused; // To be filled (but not used)
	uint32_t decoded_chunk_elemcount; // To be filled

	// Perform the actual decoding
	bool success = pfor::PatchedFrameOfReference::decode_new(
			encoded_chunk_in, remaining_encoded_length,
			decoded_chunk_out, CIIRegionEncoding::CII_CHUNK_SIZE,
			unused, decoded_chunk_elemcount, encoded_chunk_length);

	if (!success)
		return false;

	// Trim the array to fit the actual elements decoded, and move past
	// this chunk in the encoded array
	output.resize(old_elemcount + decoded_chunk_elemcount);
	encoded_chunk_it += encoded_chunk_length / sizeof(char);

	return true;
}

bool BaseProgressiveCIIDecoder::decode_next_chunk() {
	if (!is_decompressing_cii)
		return false;

	assert(cur_chunk_it == cur_chunk_it_end);

	cur_chunk.clear();

	if (!decode_next_chunk_to(cur_chunk))
		return false;

	cur_chunk_it = cur_chunk.begin();
	cur_chunk_it_end = cur_chunk.end();

	return !cur_chunk.empty();
}



ProgressiveCIIDecoder::ProgressiveCIIDecoder(const CIIRegionEncoding& cii) :
	BaseProgressiveCIIDecoder(cii), domain_size(cii.get_domain_size()), is_inverted(cii.is_inverted), next_rid(0)
{
	if (this->is_inverted) {
		this->advance_next_rid(0);
		this->decode_next_inverted_chunk();
	}
}

bool ProgressiveCIIDecoder::has_top() const {
	if (this->is_inverted)
		return this->cur_inverted_chunk_it != this->cur_inverted_chunk_it_end;
	else
		return this->BaseProgressiveCIIDecoder::has_top();
}

auto ProgressiveCIIDecoder::top() const -> rid_t {
	if (this->is_inverted)
		return *this->cur_inverted_chunk_it;
	else
		return this->BaseProgressiveCIIDecoder::top();
}

void ProgressiveCIIDecoder::next() {
	if (this->is_inverted) {
		this->cur_inverted_chunk_it++;
		if (!this->has_top())
			this->decode_next_inverted_chunk();
	} else {
		this->BaseProgressiveCIIDecoder::next();
	}
}

void ProgressiveCIIDecoder::dump_to(std::vector<rid_t>& output) {
	if (this->is_inverted) {
		// Copy any remaining decoded RIDs to the output buffer
		output.insert(output.end(), this->cur_inverted_chunk_it, this->cur_inverted_chunk_it_end);
		this->cur_inverted_chunk.clear();
		this->cur_inverted_chunk_it = this->cur_inverted_chunk.begin();
		this->cur_inverted_chunk_it_end = this->cur_inverted_chunk.end();

		// Then, decode any remaining CII chunks directly to the output buffer
		while (this->decode_next_inverted_chunk_to(output));
	} else {
		this->BaseProgressiveCIIDecoder::dump_to(output);
	}
}

bool ProgressiveCIIDecoder::decode_next_inverted_chunk() {
	assert(this->cur_inverted_chunk_it == this->cur_inverted_chunk_it_end);
	this->cur_inverted_chunk.clear();

	if (!this->decode_next_inverted_chunk_to(this->cur_inverted_chunk, 1, MAX_INVERTED_CHUNK_SIZE))
		return false;

	this->cur_inverted_chunk_it = this->cur_inverted_chunk.begin();
	this->cur_inverted_chunk_it_end = this->cur_inverted_chunk.end();

	return !this->cur_inverted_chunk.empty();
}

bool ProgressiveCIIDecoder::decode_next_inverted_chunk_to(std::vector<rid_t> &output, rid_t max_runs, rid_t max_rids) {
	if (this->next_rid == this->domain_size)
		return false;

	// Invariant: this->next_rid always points to an RID
	// that is absent in the underlying ProgressiveCIIDecoder

	for (rid_t run = 0; run < max_runs && this->next_rid < this->domain_size; ++run) {
		// Determine the next run of inverted RIDs
		const rid_t run_start_rid = this->next_rid;
		const rid_t run_end_rid =
			this->BaseProgressiveCIIDecoder::has_top() ?
				this->BaseProgressiveCIIDecoder::top() :
				this->domain_size;

		// Note: this is a bit complicated because we need to be careful about integer overflows here...
		const rid_t start_rid = run_start_rid;
		const rid_t end_rid =
			run_end_rid - run_start_rid <= max_rids ?
			run_end_rid : run_start_rid + max_rids;

		// Insert the run of RIDs
		output.insert(output.end(), boost::counting_iterator<rid_t>(start_rid), boost::counting_iterator<rid_t>(end_rid));

		// Advance this->next_rid
		this->advance_next_rid(end_rid);
	}

	return true;
}

void ProgressiveCIIDecoder::advance_next_rid(uint64_t at_least) {
	assert(this->next_rid <= at_least);
	this->next_rid = at_least;
	while (
		this->BaseProgressiveCIIDecoder::has_top() &&
		this->BaseProgressiveCIIDecoder::top() == this->next_rid)
	{
		this->BaseProgressiveCIIDecoder::next();
		++this->next_rid;
	}
}
