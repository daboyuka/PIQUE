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

#include <algorithm>
#include <vector>

#include <boost/optional.hpp>
#include <boost/none.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/logic/tribool.hpp>

#include "pique/region/cblq/cblq.hpp"
#include "pique/region/cblq/cblq-traversal.hpp"

template<int ndim>
constexpr RegionEncoding::Type CBLQRegionEncoding<ndim>::TYPE;

template<int ndim>
const typename CBLQRegionEncoding<ndim>::cblq_word_t
CBLQRegionEncoding<ndim>::ZERO_CODES_WORD = 0;

template<int ndim>
const typename CBLQRegionEncoding<ndim>::cblq_word_t
CBLQRegionEncoding<ndim>::ONE_CODES_WORD =
		((CBLQRegionEncoding<ndim>::cblq_word_t)CBLQRegionEncoding<ndim-1>::ONE_CODES_WORD << CBLQRegionEncoding<ndim-1>::BITS_PER_WORD) +
		CBLQRegionEncoding<ndim-1>::ONE_CODES_WORD;

template<>
const typename CBLQRegionEncoding<1>::cblq_word_t
CBLQRegionEncoding<1>::ONE_CODES_WORD = 0b0101;

template<int ndim>
const typename CBLQRegionEncoding<ndim>::cblq_word_t
CBLQRegionEncoding<ndim>::TWO_CODES_WORD =
		((CBLQRegionEncoding<ndim>::cblq_word_t)CBLQRegionEncoding<ndim-1>::TWO_CODES_WORD << CBLQRegionEncoding<ndim-1>::BITS_PER_WORD) +
		CBLQRegionEncoding<ndim-1>::TWO_CODES_WORD;

template<>
const typename CBLQRegionEncoding<1>::cblq_word_t
CBLQRegionEncoding<1>::TWO_CODES_WORD = 0b1010;


template<int ndim>
const typename CBLQRegionEncoding<ndim>::cblq_semiword_t
CBLQRegionEncoding<ndim>::EMPTY_SEMIWORD = 0;

template<int ndim>
const typename CBLQRegionEncoding<ndim>::cblq_semiword_t
CBLQRegionEncoding<ndim>::FULL_SEMIWORD =
		((CBLQRegionEncoding<ndim>::cblq_semiword_t)CBLQRegionEncoding<ndim-1>::FULL_SEMIWORD << CBLQRegionEncoding<ndim-1>::BITS_PER_SEMIWORD) +
		CBLQRegionEncoding<ndim-1>::FULL_SEMIWORD;

template<>
const typename CBLQRegionEncoding<1>::cblq_semiword_t
CBLQRegionEncoding<1>::FULL_SEMIWORD = 0b11;

template<> void CBLQRegionEncoding<2>::print_cblq_word(uint8_t word) {
    printf("%d%d%d%d", (word >> 0) & 0b11, (word >> 2) & 0b11, (word >> 4) & 0b11, (word >> 6) & 0b11);
}
template<> void CBLQRegionEncoding<3>::print_cblq_word(uint16_t word) {
    printf("%d%d%d%d%d%d%d%d",
    	   (word >> 0) & 0b11, (word >> 2) & 0b11 , (word >> 4) & 0b11 , (word >> 6) & 0b11,
    	   (word >> 8) & 0b11, (word >> 10) & 0b11, (word >> 12) & 0b11, (word >> 14) & 0b11);
}
template<> void CBLQRegionEncoding<4>::print_cblq_word(uint32_t word) {
    printf("%d%d%d%d%d%d%d%d%d%d%d%d%d%d%d%d",
    	   (word >> 0) & 0b11, (word >> 2) & 0b11 , (word >> 4) & 0b11 , (word >> 6) & 0b11,
    	   (word >> 8) & 0b11, (word >> 10) & 0b11, (word >> 12) & 0b11, (word >> 14) & 0b11,
    	   (word >> 16) & 0b11, (word >> 18) & 0b11 , (word >> 20) & 0b11 , (word >> 22) & 0b11,
    	   (word >> 24) & 0b11, (word >> 26) & 0b11, (word >> 28) & 0b11, (word >> 30) & 0b11);
}

template<> void CBLQRegionEncoding<2>::print_cblq_semiword(uint8_t semiword) {
    printf("%d%d%d%d",
    	   (semiword >> 0) & 0b1, (semiword >> 1) & 0b1 , (semiword >> 2) & 0b1 , (semiword >> 3) & 0b1);
}
template<> void CBLQRegionEncoding<3>::print_cblq_semiword(uint8_t semiword) {
    printf("%d%d%d%d%d%d%d%d",
    	   (semiword >> 0) & 0b1, (semiword >> 1) & 0b1 , (semiword >> 2) & 0b1 , (semiword >> 3) & 0b1,
    	   (semiword >> 4) & 0b1, (semiword >> 5) & 0b1, (semiword >> 6) & 0b1, (semiword >> 7) & 0b1);
}
template<> void CBLQRegionEncoding<4>::print_cblq_semiword(uint16_t semiword) {
    printf("%d%d%d%d%d%d%d%d%d%d%d%d%d%d%d%d",
    	   (semiword >> 0) & 0b1, (semiword >> 1) & 0b1 , (semiword >> 2) & 0b1 , (semiword >> 3) & 0b1,
    	   (semiword >> 4) & 0b1, (semiword >> 5) & 0b1, (semiword >> 6) & 0b1, (semiword >> 7) & 0b1,
    	   (semiword >> 8) & 0b1, (semiword >> 9) & 0b1 , (semiword >> 10) & 0b1 , (semiword >> 11) & 0b1,
    	   (semiword >> 12) & 0b1, (semiword >> 13) & 0b1, (semiword >> 14) & 0b1, (semiword >> 15) & 0b1);
}

template<int ndim> RegionEncoding::Type CBLQRegionEncoding<ndim>::get_type() const { return RegionEncoding::Type::UNKNOWN; }
template<> RegionEncoding::Type CBLQRegionEncoding<1>::get_type() const { return RegionEncoding::Type::CBLQ_1D; }
template<> RegionEncoding::Type CBLQRegionEncoding<2>::get_type() const { return RegionEncoding::Type::CBLQ_2D; }
template<> RegionEncoding::Type CBLQRegionEncoding<3>::get_type() const { return RegionEncoding::Type::CBLQ_3D; }
template<> RegionEncoding::Type CBLQRegionEncoding<4>::get_type() const { return RegionEncoding::Type::CBLQ_4D; }

//template<int ndim>
//boost::shared_ptr< CBLQRegionEncoding<ndim> >
//CBLQRegionEncoding<ndim>::make_uniform_region(uint64_t nelem, bool filled) {
//	boost::shared_ptr< CBLQRegionEncoding<ndim> > uregion = boost::make_shared< CBLQRegionEncoding<ndim> >(nelem);
//
//	int levels = 1;
//	while (nelem > (1ULL << (levels * ndim)))
//		levels++;
//
//	uregion->words.push_back(filled ?
//					CBLQRegionEncoding<ndim>::ONE_CODES_WORD :
//					CBLQRegionEncoding<ndim>::ZERO_CODES_WORD);
//
//	uregion->level_lens.push_back(1);
//	uregion->level_lens.insert(uregion->level_lens.end(), levels - 1, 0);
//
//	uregion->has_dense_suffix = false;
//	return uregion;
//}

// Returns true if all CBLQs are compatible with a dense suffix, false if with a non-dense suffix, and
// indeterminate if the CBLQs have incompatible suffixes
// (defaults to false if all CBLQs are compatible but have indeterminate suffix types)
template<int ndim>
boost::optional< bool >
CBLQRegionEncoding<ndim>::deduce_common_suffix_density(const CBLQRegionEncoding<ndim> &c1, const CBLQRegionEncoding<ndim> &c2) {
	if (c1.is_suffix_empty() && c2.is_suffix_empty())
		return boost::make_optional(false); // both suffixes are empty; pick an arbitrary density (non-dense)
	else if (c1.is_suffix_empty() || c2.is_suffix_empty()) // known: at most one suffix is empty
		return boost::make_optional(c1.get_suffix_density(c2.get_suffix_density())); // return the density of the first operand, with preference to the density of the second if the first is empty
	else if (c1.get_suffix_density() != c2.get_suffix_density()) // known: neither suffix is empty
		return boost::none; // suffix densities disagree: error
	else // known: suffix densities are non-empty and equal
		return boost::make_optional(c1.get_suffix_density()); // both suffix densities agree: return that density
}

template<int ndim>
boost::optional< bool >
CBLQRegionEncoding<ndim>::deduce_common_suffix_density(
		typename std::vector< boost::shared_ptr< const CBLQRegionEncoding<ndim> > >::const_iterator begin,
		typename std::vector< boost::shared_ptr< const CBLQRegionEncoding<ndim> > >::const_iterator end)
{
	boost::logic::tribool current_suffix_type = boost::logic::indeterminate;

	for (auto it = begin; it != end; ++it) {
		const CBLQRegionEncoding<ndim> &cblq = **it;

		if (cblq.is_suffix_empty())
			; // this CBLQ does not affect the current type; do nothing
		else if (boost::logic::indeterminate(current_suffix_type)) // precondition: CBLQ is non-empty
			current_suffix_type = cblq.get_suffix_density(); // update current type to match this CBLQ's type
		else if (cblq.get_suffix_density() != current_suffix_type) // precondition: both are non-empty
			return boost::none; // error: this CBLQ does not match the current type
		else // precondition: suffix densities are non-empty and equal
			; // this CBLQ is compatible with the current type; do nothing
	}

	return boost::make_optional<bool>((bool)current_suffix_type); // true->true, false->false, indeterminate(i.e., all empty)->false
}


template<int ndim>
size_t CBLQRegionEncoding<ndim>::get_size_in_bytes() const {
	if (has_dense_suffix) {
		return ((words.size() * BITS_PER_WORD + 7) >> 3) +
			   ((dense_suffix->get_num_semiwords() * BITS_PER_SEMIWORD + 7) >> 3);
	} else {
		return (words.size() * BITS_PER_WORD + 7) >> 3;
	}
}

// dump()
template<int ndim>
void CBLQRegionEncoding<ndim>::dump() const {
	typedef typename std::vector<cblq_word_t>::const_iterator word_iter_t;

	std::cout << "Domain size: " << this->domain_size << std::endl;

	int mod = 0;
	for (word_iter_t it = this->words.begin(); it != this->words.end(); it++) {
		CBLQRegionEncoding<ndim>::print_cblq_word(*it);
		if ((++mod %= 16) == 0)
			printf("\n");
		else
			printf(" ");
	}

	if (has_dense_suffix) {
		printf("D[ ");
		dense_suffix->dump();
		printf("]");
	}
}

/*
template<int ndim>
uint64_t CBLQRegionEncoding<ndim>::get_element_count() const {
	const uint64_t domain_size = this->domain_size;
	uint64_t elem_count = 0;

	using block_offset_t = typename CBLQTraversalBlocks<ndim>::block_offset_t;
	using block_size_t = typename CBLQTraversalBlocks<ndim>::block_size_t;

	auto process_block_fn = [&elem_count, domain_size](block_size_t block_size, block_offset_t block_offset) {
		block_offset_t block_end_offset = std::min(block_offset + block_size, domain_size);
		if (block_end_offset > block_offset)
			elem_count += block_end_offset - block_offset;
	};

	CBLQTraversalBlocks<ndim>::template traverse_bf<true>(*this, process_block_fn);
	return elem_count;
}
*/

template<int ndim>
uint64_t CBLQRegionEncoding<ndim>::get_element_count() const {
	const uint64_t domain_size = this->domain_size;
	uint64_t elem_count = 0;

	using block_offset_t = typename CBLQTraversalBlocks<ndim>::block_offset_t;
	using block_size_t = typename CBLQTraversalBlocks<ndim>::block_size_t;

	auto process_block_fn = [domain_size, &elem_count](block_size_t block_size, block_offset_t block_offset) {
		block_offset_t block_end_offset = std::min(block_offset + block_size, domain_size);
		if (block_end_offset > block_offset)
			elem_count += block_end_offset - block_offset;
	};

	auto process_leaf_block_fn = [domain_size, &elem_count](block_size_t /*block_size == 1, unused*/, block_offset_t block_offset) -> void {
		if (block_offset < domain_size)
			++elem_count;
	};

	CBLQTraversalBlocks<ndim>::template traverse_df_leafopt<true>(*this, process_block_fn, process_leaf_block_fn);
	return elem_count;
}

template<int ndim>
void CBLQRegionEncoding<ndim>::convert_to_rids_unsorted(std::vector<uint32_t> &out, bool preserve_self) {
	const uint64_t domain_size = this->domain_size;
	out.clear();

	using block_offset_t = typename CBLQTraversalBlocks<ndim>::block_offset_t;
	using block_size_t = typename CBLQTraversalBlocks<ndim>::block_size_t;

	auto exec_inner_block_loop_fn = [&out, domain_size](int levels, int level, uint64_t level_len, block_size_t level_block_size, typename CBLQTraversalBlocks<ndim>::InnerBlockLoopParameters inner_block_loop_params) {
		if (level_block_size == 1) {
			auto process_block_fn = [&out, domain_size](block_size_t block_size, block_offset_t block_offset) -> void {
				if (block_offset < domain_size)
					out.push_back(block_offset);
			};

			CBLQTraversalBlocks<ndim>::template traverse_bf_inner_loop<true>(process_block_fn, inner_block_loop_params);
		} else {
			auto process_block_fn = [&out, domain_size](block_size_t block_size, block_offset_t block_offset) -> void {
				const block_offset_t block_end_offset = std::min(block_offset + block_size, domain_size);
				if (block_end_offset > block_offset) {
					out.insert(
							out.end(),
							boost::counting_iterator<uint32_t>(static_cast<uint32_t>(block_offset)),
							boost::counting_iterator<uint32_t>(static_cast<uint32_t>(block_end_offset))
					);
				}
			};

			CBLQTraversalBlocks<ndim>::template traverse_bf_inner_loop<true>(process_block_fn, inner_block_loop_params);
		}
	};

	CBLQTraversalBlocks<ndim>::template traverse_bf_outer_loop(*this, exec_inner_block_loop_fn);
}

template<int ndim>
std::pair<size_t, size_t>
CBLQRegionEncoding<ndim>::compute_dense_prefix_suffix() const {
	typedef typename std::vector<cblq_word_t>::const_iterator word_iter_t;

	word_iter_t first_nondense_prefix_word = words.begin();
	word_iter_t last_nondense_suffix_word = words.begin();

	for (word_iter_t it = this->words.begin(); it != this->words.end(); it++) {
		const cblq_word_t word = *it;

		if (first_nondense_prefix_word == words.begin() && (word & ONE_CODES_WORD) != 0)
			first_nondense_prefix_word = it; // We found the first nondense prefix word (has 1-codes)

		if ((word & TWO_CODES_WORD) != 0)
			last_nondense_suffix_word = it;
	}

	const size_t dense_prefix_len = first_nondense_prefix_word - words.begin();
	const size_t dense_suffix_len = words.end() - (last_nondense_suffix_word + 1);

	return std::pair<size_t, size_t>(dense_prefix_len, dense_suffix_len);
}

template<typename cblq_word_t>
struct upmerge {
	upmerge(size_t pos, bool is_one_word) :
		child_pos(pos),
		is_one_word(is_one_word),
		patch_mask(is_one_word ? 0b11 : 0b10)
	{}

	size_t child_pos;
	bool is_one_word;
	cblq_word_t patch_mask;
};

template<int ndim>
void
CBLQRegionEncoding<ndim>::compact() {
	// Special case: if there is no compacting to do, stop now (single-level
	// dense-suffix CBLQs trigger an edge case that we can simply avoid with this check)
	if (this->level_lens.size() < 2)
		return;

	const int non_dense_levels = this->level_lens.size() - (this->has_dense_suffix ? 1 : 0);

	size_t last_level_len = 0;
	std::vector< upmerge<cblq_word_t> > next_level_upmerges;
	std::vector< upmerge<cblq_word_t> > this_level_upmerges;
	auto this_level_upmerges_it = this_level_upmerges.begin();

	// If we have a dense suffix, compact it first
	// Since we don't need to do patching on the leaf layer, we can iterate
	// through the (semi)words forward and push upmerges forward (instead of
	// both backward, as below)
	if (has_dense_suffix) {
		typename CBLQSemiwords<ndim>::iterator in_it = dense_suffix->begin();
		typename CBLQSemiwords<ndim>::iterator out_it = dense_suffix->begin();

		// word_pos = the index of the current word in the current level
		for (size_t word_pos = 0; in_it.has_top(); in_it.next(), word_pos++) {
			cblq_semiword_t cur_word = in_it.top();

			if (cur_word == EMPTY_SEMIWORD) {
				next_level_upmerges.push_back(upmerge<cblq_word_t>(word_pos, false));
			} else if (cur_word == FULL_SEMIWORD) {
				next_level_upmerges.push_back(upmerge<cblq_word_t>(word_pos, true));
			} else {
				out_it.set(cur_word);
				out_it.next();
			}
		}

		// Record this level's (original) length
		last_level_len = dense_suffix->get_num_semiwords();

		// Trim the dense suffix, eliminating any compacted semiwords
		dense_suffix->trim(out_it.get_pos());

		// Reverse next_level_upmerges, since we iterated forward instead of backward over this level
		std::reverse(next_level_upmerges.begin(), next_level_upmerges.end());
	}

	std::vector<cblq_word_t> &words = this->words;
	auto in_it = words.rbegin();
	auto out_it = words.rbegin();

	for (int level = non_dense_levels - 1; level >= 0; level--) {
		// Get this level's size
		const size_t level_len = this->level_lens[level];

		// Prepare the upmerges for this level, and clear the buffer for the next level
		this_level_upmerges = std::move(next_level_upmerges);
		this_level_upmerges.emplace_back(std::numeric_limits<size_t>::max(), false); // Add a sentinel so an additional check isn't needed during the iteration
		this_level_upmerges_it = this_level_upmerges.begin();
		next_level_upmerges.clear();

		// word_pos is the index of the current word within the current level (we start at the end)
		// child_pos is the number of 2-codes to the left of the current code in the current word, which corresponds to the word_pos of the next-lower level
		size_t child_pos = last_level_len;
		for (size_t word_pos = level_len - 1; word_pos != (size_t)-1; word_pos--) {
			cblq_word_t cur_word = *in_it++;

			// Count the two-codes in the current word, and patch any that are marked for patching
			if (cur_word & TWO_CODES_WORD) {
				for (int i = BITS_PER_WORD - BITS_PER_CODE; i >= 0; i -= BITS_PER_CODE) {
					if ((cur_word >> i) & 0b10) {
						child_pos--;
						if (child_pos == this_level_upmerges_it->child_pos) {
							cur_word ^= (this_level_upmerges_it->patch_mask << i);
							this_level_upmerges_it++;
						}
					}
				}
			}

			// Check if this word is a "pure word," in which case an upmerge is registered to later patch a two-code
			if (cur_word == ZERO_CODES_WORD) {
				next_level_upmerges.push_back(upmerge<cblq_word_t>(word_pos, false));
				this->level_lens[level]--; // We just deleted a word
			} else if (cur_word == ONE_CODES_WORD) {
				next_level_upmerges.push_back(upmerge<cblq_word_t>(word_pos, true));
				this->level_lens[level]--; // We just deleted a word
			} else {
				*out_it++ = cur_word;
			}
		}

		last_level_len = level_len;

		this_level_upmerges.pop_back(); // Delete the sentinel
		assert(this_level_upmerges_it == this_level_upmerges.end());
	}

	// Erase leftover "junk" elements at the beginning, now that compaction is complete
	words.erase(words.begin(), out_it.base());

	// If we upmerged the final word (which happens only if the CBLQ represents a completely
	// white or black region), re-add a single zero-word or one-word as a special case
	if (words.size() == 0) {
		this->level_lens[0] = 1;
		this->words.push_back(next_level_upmerges.front().is_one_word ? ONE_CODES_WORD : ZERO_CODES_WORD);
		next_level_upmerges.pop_back();
	}
	assert(next_level_upmerges.empty());
}

template<int ndim>
bool
CBLQRegionEncoding<ndim>::is_single_word_cblq(cblq_word_t expected_single_word) const {
	// Ensure there is only one CBLQ word
	if (words.size() != 1)
		return false;

	// Ensure that one word is equal to 00..0
	if (words[0] != expected_single_word)
		return false;

	auto it = level_lens.cbegin();
	if (*it++ != 1)
		return false;
	while (it != level_lens.cend())
		if (*it++ != 0)
			return false;

	// Ensure there is no dense suffix, or if there is, that it is empty
	if (has_dense_suffix)
		if (dense_suffix->get_num_semiwords() != 0)
			return false;

	return true;
}

template<int ndim>
bool
CBLQRegionEncoding<ndim>:: is_the_empty_region() const { return this->is_single_word_cblq(CBLQRegionEncoding<ndim>::ZERO_CODES_WORD); }

template<int ndim>
bool
CBLQRegionEncoding<ndim>:: is_the_filled_region() const { return this->is_single_word_cblq(CBLQRegionEncoding<ndim>::ONE_CODES_WORD); }


template<int ndim>
bool
CBLQRegionEncoding<ndim>::operator==(const RegionEncoding &other_base) const {
	if (typeid(other_base) != typeid(CBLQRegionEncoding<ndim>))
		return false;
	const CBLQRegionEncoding<ndim> &other = dynamic_cast<const CBLQRegionEncoding<ndim>&>(other_base);

	if (this->domain_size != other.domain_size)
		return false;
	if (this->has_dense_suffix != other.has_dense_suffix)
		return false;
	if (this->level_lens != other.level_lens ||
		this->words != other.words)
		return false;
	if (this->has_dense_suffix)
		if (*this->dense_suffix != *other.dense_suffix)
			return false;
	return true;
}

// Explicit instantiation of 2D-4D CBLQ region encodings
template class CBLQRegionEncoding<2>;
template class CBLQRegionEncoding<3>;
template class CBLQRegionEncoding<4>;
