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
 * cii-decoder.hpp
 *
 *  Created on: Mar 22, 2014
 *      Author: David A. Boyuka II
 */
#ifndef CII_DECODER_HPP_
#define CII_DECODER_HPP_

#include <vector>

#include "pique/region/cii/cii.hpp"

/*
 * Note: ignores the cii->is_inverted flag
 */
class BaseProgressiveCIIDecoder {
public:
	typedef CIIRegionEncoding::rid_t rid_t;
public:
	BaseProgressiveCIIDecoder(const CIIRegionEncoding &cii);

	bool has_top() const;
	rid_t top() const;
	void next();

	void dump_to(std::vector<rid_t> &output);

private:
	bool decode_next_chunk();
	bool decode_next_chunk_to(std::vector<rid_t> &output);
private:
	const bool is_decompressing_cii;

	std::vector<rid_t> cur_chunk;
	std::vector<rid_t>::const_iterator cur_chunk_it;
	std::vector<rid_t>::const_iterator cur_chunk_it_end;

	std::vector<char>::const_iterator encoded_chunk_it;
	std::vector<char>::const_iterator encoded_chunk_it_end;
};

/*
 * Note: respects the cii->is_inverted flag
 */
class ProgressiveCIIDecoder : public BaseProgressiveCIIDecoder {
public:
	using BaseProgressiveCIIDecoder::rid_t;
public:
	ProgressiveCIIDecoder(const CIIRegionEncoding &cii);

	bool has_top() const;
	rid_t top() const;
	void next();

	void dump_to(std::vector<rid_t> &output);

private:
	static const rid_t MAX_INVERTED_CHUNK_SIZE = ((rid_t)1 << 10);

	// post-condition: "this->next_rid" is advanced to the first RID that is not in the underlying ProgressiveCIIDecoder, and that is at least "at_least"
	void advance_next_rid(uint64_t at_least);
	bool decode_next_inverted_chunk();
	bool decode_next_inverted_chunk_to(std::vector<rid_t> &output, rid_t max_runs = std::numeric_limits<rid_t>::max(), rid_t max_rids = std::numeric_limits<rid_t>::max());
private:
	const uint64_t domain_size;
	const bool is_inverted;
	uint64_t next_rid;

	std::vector<rid_t> cur_inverted_chunk;
	std::vector<rid_t>::const_iterator cur_inverted_chunk_it;
	std::vector<rid_t>::const_iterator cur_inverted_chunk_it_end;
};

#endif /* CII_DECODER_HPP_ */
