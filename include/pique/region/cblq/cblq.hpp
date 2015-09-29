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
 * cblq.hpp
 *
 *  Created on: Jan 22, 2014
 *      Author: David A. Boyuka II
 */

#ifndef CBLQ_HPP_
#define CBLQ_HPP_

#include <iostream>
#include <vector>
#include <functional>
#include <boost/smart_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/optional.hpp>

#include "pique/region/region-encoding.hpp"

#include "pique/util/compute-exp-level.hpp"
#include "pique/util/fixed-archive.hpp"

// Used to map ndim->cblq_word_t. Declared but not defined; only specialized versions allowed.
template<int ndim> struct CBLQWord;
template<> struct CBLQWord<1> { typedef uint8_t cblq_word_t; typedef uint8_t cblq_semiword_t; };
template<> struct CBLQWord<2> { typedef uint8_t cblq_word_t; typedef uint8_t cblq_semiword_t; };
template<> struct CBLQWord<3> { typedef uint16_t cblq_word_t; typedef uint8_t cblq_semiword_t; };
template<> struct CBLQWord<4> { typedef uint32_t cblq_word_t; typedef uint16_t cblq_semiword_t; };

template<int ndim>
class CBLQSemiwords;

template<int ndim> constexpr RegionEncoding::Type get_cblq_rep_type() { return RegionEncoding::Type::UNKNOWN; }
template<> constexpr RegionEncoding::Type get_cblq_rep_type<1>() { return RegionEncoding::Type::CBLQ_1D; }
template<> constexpr RegionEncoding::Type get_cblq_rep_type<2>() { return RegionEncoding::Type::CBLQ_2D; }
template<> constexpr RegionEncoding::Type get_cblq_rep_type<3>() { return RegionEncoding::Type::CBLQ_3D; }
template<> constexpr RegionEncoding::Type get_cblq_rep_type<4>() { return RegionEncoding::Type::CBLQ_4D; }

// CBLQRegionEncoding, a concrete subclass of RegionEncoding utilizing a CBLQ encoding
template<int ndim>
class CBLQRegionEncoding : public RegionEncoding {
public:
	typedef typename CBLQWord<ndim>::cblq_word_t cblq_word_t;
	typedef typename CBLQWord<ndim>::cblq_semiword_t cblq_semiword_t;

	static constexpr RegionEncoding::Type TYPE = get_cblq_rep_type<ndim>();

	static const int BITS_PER_CODE = 2;
	static const int CODES_PER_WORD = (1 << ndim);
	static const int BITS_PER_WORD = BITS_PER_CODE * CODES_PER_WORD;
	static const int BITS_PER_SEMIWORD = CODES_PER_WORD * 1;
    static const cblq_word_t ZERO_CODES_WORD;
    static const cblq_word_t ONE_CODES_WORD;
    static const cblq_word_t TWO_CODES_WORD;
    static const cblq_semiword_t EMPTY_SEMIWORD;
    static const cblq_semiword_t FULL_SEMIWORD;

	static void print_cblq_word(cblq_word_t word);
	static void print_cblq_semiword(cblq_semiword_t semiword);
public:
	//static boost::shared_ptr< CBLQRegionEncoding<ndim> > make_uniform_region(uint64_t nelem, bool filled);
	static boost::optional< bool > deduce_common_suffix_density(const CBLQRegionEncoding<ndim> &c1, const CBLQRegionEncoding<ndim> &c2);
	static boost::optional< bool > deduce_common_suffix_density(typename std::vector< boost::shared_ptr< const CBLQRegionEncoding<ndim> > >::const_iterator begin, typename std::vector< boost::shared_ptr< const CBLQRegionEncoding<ndim> > >::const_iterator end);

public:
	CBLQRegionEncoding() :
		RegionEncoding(),
		words(), level_lens(),
		has_dense_suffix(false),
		dense_suffix(new CBLQSemiwords<ndim>(false))
	{}
	CBLQRegionEncoding(uint64_t nelem) :
		RegionEncoding(nelem),
		words(), level_lens(),
		has_dense_suffix(false),
		dense_suffix(new CBLQSemiwords<ndim>(false))
	{}
	CBLQRegionEncoding(uint64_t nelem, bool filled) :
		RegionEncoding(nelem),
		words(1, filled ? ONE_CODES_WORD : ZERO_CODES_WORD), level_lens(compute_exp_level<ndim>(nelem), 0),
		has_dense_suffix(false),
		dense_suffix(new CBLQSemiwords<ndim>(false))
	{
		this->level_lens[0] = (nelem > 0) ? 1 : 0;
	}
	CBLQRegionEncoding(CBLQRegionEncoding &&) noexcept = default; // Explicitly allow move construction
	virtual ~CBLQRegionEncoding() {}

	CBLQRegionEncoding & operator=(CBLQRegionEncoding &&) = default; // Explicitly allow move assignment, since the default move assignment is implicitly deleted due to the presence of a non-default destructor

	virtual RegionEncoding::Type get_type() const;

    virtual size_t get_size_in_bytes() const;
    virtual void dump() const;

	virtual uint64_t get_element_count() const;
	virtual void convert_to_rids(std::vector< uint32_t > &out, bool sorted = false, bool preserve_self = true);
	virtual void convert_to_rids(std::vector< uint64_t > &out, uint64_t offset, bool sorted = false, bool preserve_self = true);

    int get_ndim() const { return ndim; }
    size_t get_num_words() const {
    	if (has_dense_suffix)
    		return words.size() + dense_suffix->get_num_semiwords();
    	else
    		return words.size();
    }
    int get_num_levels() const { return level_lens.size(); }
    size_t get_level_num_words(int level) {
    	if (level == level_lens.size() - 1 && has_dense_suffix)
    		return dense_suffix->get_num_semiwords();
    	else
    		return level_lens[level];
    }

    std::pair<size_t, size_t> compute_dense_prefix_suffix() const;
    void compact();

    bool is_the_empty_region() const; // Does this CBLQ represent a totally empty region?
    bool is_the_filled_region() const; // Does this CBLQ represent a totally filled region?

    bool is_suffix_empty() const { return this->level_lens.back() == 0 && this->dense_suffix->is_empty(); }
    bool get_suffix_density(bool preferred = false) const { return this->is_suffix_empty() ? preferred : this->has_dense_suffix; }

	virtual void save_to_stream(std::ostream &out) { boost::archive::simple_binary_oarchive(out, boost::archive::no_header) << *this; }
	virtual void load_from_stream(std::istream &in) { boost::archive::simple_binary_iarchive(in, boost::archive::no_header) >> *this; }

	virtual bool operator==(const RegionEncoding &other) const;

private:
	bool is_single_word_cblq(cblq_word_t expected_single_word) const;

	template<typename LevelStateT, typename QueueElemT, typename LevelStateGenFn, typename ProcessCBLQCodeFn>
	void iterate_over_cblq(LevelStateGenFn level_state_gen, QueueElemT first_queue_elem, ProcessCBLQCodeFn process_code_fn) const;

	static inline void unpack_cblq_word(typename CBLQRegionEncoding<ndim>::cblq_word_t word, char *codes) {
		for (int code_pos = 0; code_pos < CBLQRegionEncoding<ndim>::CODES_PER_WORD; code_pos++)
			codes[code_pos] = (word & (0b11 << code_pos * CBLQRegionEncoding<ndim>::BITS_PER_CODE));
	}

	void convert_to_rids_unsorted(std::vector<uint32_t> &out, bool preserve_self);

	template<typename rid_t, bool has_offset>
	void convert_to_rids_sorted(std::vector< rid_t > &out, uint64_t offset, bool preserve_self);

private:
    friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	ar & domain_size;
    	ar & has_dense_suffix;
    	ar & level_lens;
    	ar & words;
    	if (has_dense_suffix)
    		ar & *dense_suffix;
    }

    // Used by any code inserting new content into this CBLQ.
    // Note: does NOT leave valid region representing the empty region;
    // that requires a non-empty internal state (one CBLQ word: 00...0)
    void clear() {
    	words.clear();
    	level_lens.clear();
    	dense_suffix->clear();
    	has_dense_suffix = false;
    	domain_size = 0;
    }

private:
    std::vector<cblq_word_t> words;
    std::vector<size_t> level_lens; // level_lens[0] refers to the highest level (i.e., the one with exactly one word)

    bool has_dense_suffix;
    std::unique_ptr< CBLQSemiwords<ndim> > dense_suffix;

    template<int ndim2> friend class CBLQRegionEncoder;
    template<int ndim2> friend class CBLQRegionIO;
    template<int ndim2> friend class CBLQSetOperations;
    template<int ndim2> friend class CBLQSetOperationsFast;
    template<int ndim2> friend class CBLQSetOperationsNAry1;
    template<int ndim2> friend class CBLQSetOperationsNAry2;
    template<int ndim2> friend class CBLQSetOperationsNAry2Dense;
    template<int ndim2> friend class CBLQSetOperationsNAry3Dense;
    template<int ndim2> friend class CBLQSetOperationsNAry3Fast;
    template<int ndim2> friend class CBLQToBitmapConverter;
    template<int ndim2, typename QueueElemT> friend class CBLQTraversal;
    template<int ndim2> friend class CBLQDepthFirstTraversalAccess;
};

template<> class RegionEncoding::TypeToClass<RegionEncoding::Type::CBLQ_2D> { typedef CBLQRegionEncoding<2> REClass; };
template<> class RegionEncoding::TypeToClass<RegionEncoding::Type::CBLQ_3D> { typedef CBLQRegionEncoding<3> REClass; };
template<> class RegionEncoding::TypeToClass<RegionEncoding::Type::CBLQ_4D> { typedef CBLQRegionEncoding<4> REClass; };

// Instantiations for 2D-4D
BOOST_CLASS_IMPLEMENTATION(CBLQRegionEncoding<2>, boost::serialization::object_serializable) // no version information serialized
BOOST_CLASS_IMPLEMENTATION(CBLQRegionEncoding<3>, boost::serialization::object_serializable) // no version information serialized
BOOST_CLASS_IMPLEMENTATION(CBLQRegionEncoding<4>, boost::serialization::object_serializable) // no version information serialized

// Class for handling the dense prefix/suffix structure (since CBLQSemiwords
// are too small for even a char at ndim == 1 or 2)
// TODO: Consider generalizing this to a sub-byte word stream, could be useful
//       for 1D CBLQ as well
template<int ndim>
class CBLQSemiwords {
public:
	typedef typename CBLQRegionEncoding<ndim>::cblq_word_t cblq_word_t;
	typedef typename CBLQRegionEncoding<ndim>::cblq_semiword_t cblq_semiword_t;

public:
	class iterator {
	private:
		iterator(CBLQSemiwords &dw, size_t pos);

	public:
		bool has_top() const { return pos < dw.num_semiwords; }
		size_t get_pos() { return pos; }

		cblq_semiword_t top() const;
		cblq_word_t top_fullword() const;

		void next();
		void set(cblq_semiword_t semiword);
		void set_fullword(cblq_word_t fullword);

	protected:
		CBLQSemiwords<ndim> &dw;
		size_t pos;
		size_t block_pos;
		int intrablock_pos;

		template<int ndim2> friend class CBLQSemiwords;
	};

public:
	CBLQSemiwords(bool encoding_two_codes) :
		encoding_two_codes(encoding_two_codes),
		num_semiwords(0),
		semiwords(1, 0) // Ensure at least one block is present
	{}
	CBLQSemiwords(CBLQSemiwords &&) noexcept = default; // Explicitly allow move construction

	CBLQSemiwords & operator=(CBLQSemiwords &&) = default; // Explicitly allow move assignment

	bool is_empty() const { return this->num_semiwords == 0; }
	size_t get_num_semiwords() const { return num_semiwords; }

	cblq_semiword_t get(size_t pos);
	void set(size_t pos, cblq_semiword_t value);
	iterator iterator_at(size_t pos = 0) { return iterator(*this, pos); }
	iterator begin() { return iterator_at(0); }
	iterator end() { return iterator_at(num_semiwords); }

	void push(cblq_semiword_t semiword) { end().set(semiword); }
	void push_fullword(cblq_word_t fullword) { end().set_fullword(fullword); }

	void trim(size_t new_num_semiwords);
	void expand(size_t new_num_semiwords);
	void ensure_padded(); // Ensures at least one empty block is available in the semiwords array
	void append(CBLQSemiwords &other);

	void clear();

    void dump() const;

    bool operator==(const CBLQSemiwords<ndim> &other);
    bool operator!=(const CBLQSemiwords<ndim> &other) { return !(*this == other); }

private:
    friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	ar & num_semiwords;

    	if (num_semiwords > 0) {
    		const size_t num_blocks_needed = (num_semiwords - 1) / SEMIWORDS_PER_BLOCK + 1;
    		const size_t num_bytes_needed = (num_semiwords * CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD - 1) / 8 + 1;
    		// Read/write the minimum number of bytes
    		if (Archive::is_loading::value) {
    			semiwords.clear();
    			semiwords.resize(num_blocks_needed);
    		}
    		ar & boost::serialization::make_binary_object(
    				(void*)&semiwords.front(),
    				num_bytes_needed);
    	} else {
    		if (Archive::is_loading::value) {
    			semiwords.clear();
    			semiwords.resize(1, 0);
    		}
    	}
    }

private:
    typedef uint64_t block_t;

	static const cblq_semiword_t SEMIWORD_MASK = (1ULL << CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD) - 1;
	static const int BITS_PER_BLOCK = sizeof(block_t) << 3;
	static const int SEMIWORDS_PER_BLOCK = BITS_PER_BLOCK / CBLQRegionEncoding<ndim>::BITS_PER_SEMIWORD;

private:
    const bool encoding_two_codes; // If true, set bits represent two-codes; one-codes otherwise

    size_t num_semiwords; // In CBLQSemiwords, which are of length 2^ndim bits each
	std::vector<block_t> semiwords;

	template<int ndim2> friend class CBLQSetOperationsFast;
	template<int ndim2> friend class CBLQSetOperationsNAry3Fast;
	template<int ndim2, typename QueueElemT> friend class CBLQTraversal;
	template<int ndim2> friend class CBLQDepthFirstTraversalAccess;
};

// LevelStateGenFn(levels, level, level_len)
// ProcessCBLQCodeFn(code, level, const &level_state, const &queue_elem, codepos, &queue_it)
template<int ndim>
template<typename LevelStateT, typename QueueElemT, typename LevelStateGenFn, typename ProcessCBLQCodeFn>
void CBLQRegionEncoding<ndim>::iterate_over_cblq(LevelStateGenFn level_state_gen, QueueElemT first_queue_elem, ProcessCBLQCodeFn process_code_fn) const {
	std::vector< QueueElemT > this_level_queue(1, first_queue_elem);
	std::vector< QueueElemT > next_level_queue;

	char word_codes[CBLQRegionEncoding<ndim>::CODES_PER_WORD];

	const int levels = this->get_num_levels();
	const bool has_dense_suffix = this->has_dense_suffix;
	const int non_dense_levels = has_dense_suffix ? levels - 1 : levels;
	const uint64_t last_level_len = (has_dense_suffix ? this->dense_suffix->get_num_semiwords() : this->level_lens[levels - 1]);

	auto cblq_word_it = this->words.cbegin();

	for (int level = 0; level < non_dense_levels; level++) {
		const uint64_t level_len = this->level_lens[level];
		const uint64_t next_level_len = (level + 1 == levels - 1 ? last_level_len : level + 1 == levels ? 0 : this->level_lens[level + 1]);

		const LevelStateT level_state = level_state_gen(levels, level, level_len);

		next_level_queue.resize(next_level_len);
		auto queue_it = this_level_queue.cbegin();
		auto next_queue_it = next_level_queue.begin();

		for (uint64_t word_pos = 0; word_pos < level_len; ++word_pos, ++cblq_word_it, ++queue_it) {
			const cblq_word_t cblq_word = *cblq_word_it;
			CBLQRegionEncoding<ndim>::unpack_cblq_word(cblq_word, word_codes);

			const QueueElemT &queue_elem = *queue_it;

			for (int i = 0; i < CBLQRegionEncoding<ndim>::CODES_PER_WORD; i++)
				process_code_fn(word_codes[i], level, level_state, queue_elem, i, next_queue_it);
		}

		assert(next_queue_it == next_level_queue.end());

		this_level_queue = std::move(next_level_queue);
		next_level_queue.clear();
	}

	if (this->has_dense_suffix) {
		const LevelStateT level_state = level_state_gen(levels, non_dense_levels, this->dense_suffix->get_num_semiwords());

		auto dense_word_it = this->dense_suffix->begin();
		auto queue_it = this_level_queue.cbegin();
		auto next_queue_it = next_level_queue.begin(); // Should never be accessed

		while (dense_word_it.has_top()) {
			cblq_semiword_t semiword = dense_word_it.top();
			dense_word_it.next();

			const QueueElemT &queue_elem = *queue_it;
			++queue_it;

			for (int i = 0; i < CBLQRegionEncoding<ndim>::CODES_PER_WORD; i++) {
				process_code_fn(semiword & 0b1, levels - 1, level_state, queue_elem, i, next_queue_it);
				semiword >>= 1;
			}
		}

		assert(next_queue_it == next_level_queue.begin());
	}
}

#endif /* CBLQ_HPP_ */
