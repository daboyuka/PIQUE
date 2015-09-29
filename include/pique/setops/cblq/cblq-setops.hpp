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
 * cblq-setops.hpp
 *
 *  Created on: Mar 25, 2014
 *      Author: David A. Boyuka II
 */
#ifndef CBLQ_SETOPS_HPP_
#define CBLQ_SETOPS_HPP_

#include <cstdlib>
#include <vector>
#include <boost/smart_ptr.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/cblq/cblq.hpp"
#include "pique/setops/setops.hpp"

struct CBLQSetOperationsConfig {
	CBLQSetOperationsConfig(bool compact_after_setop, bool no_compact_during_nary = true) :
		compact_after_setop(compact_after_setop), no_compact_during_nary(no_compact_during_nary)
	{}

	bool compact_after_setop;
	bool no_compact_during_nary;
};

template<int ndim>
class CBLQSetOperations : public SetOperations< CBLQRegionEncoding<ndim> > {
public:
	static constexpr int NDIM = ndim;

	typedef CBLQSetOperationsConfig SetOperationsConfig;
	CBLQSetOperations(CBLQSetOperationsConfig conf) : conf(conf), suppress_compact(false) {}
	virtual ~CBLQSetOperations() {}

protected:
    using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;
    using typename SetOperations< CBLQRegionEncoding<ndim> >::RegionEncodingCPtrCIter;

private:
    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > unary_set_op_impl(boost::shared_ptr< const CBLQRegionEncoding<ndim> > region, UnarySetOperation op) const;
    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > binary_set_op_impl(boost::shared_ptr< const CBLQRegionEncoding<ndim> > left, boost::shared_ptr< const CBLQRegionEncoding<ndim> > right, NArySetOperation op) const;

    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > nary_set_op_impl(RegionEncodingCPtrCIter region_it, RegionEncodingCPtrCIter region_end_it, NArySetOperation op) const;

protected:
    const CBLQSetOperationsConfig conf;
    mutable bool suppress_compact;

    template<int ndim2> friend class CBLQSetOperationsFast; // Allows delegation upward to this class
};

template<int ndim>
class CBLQSetOperations2 : public CBLQSetOperations<ndim> {
public:
	CBLQSetOperations2(CBLQSetOperationsConfig conf) : CBLQSetOperations<ndim>(conf) {}
	virtual ~CBLQSetOperations2() {}

private:
    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > unary_set_op_impl(boost::shared_ptr< const CBLQRegionEncoding<ndim> > region, UnarySetOperation op) const;
    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > binary_set_op_impl(boost::shared_ptr< const CBLQRegionEncoding<ndim> > left, boost::shared_ptr< const CBLQRegionEncoding<ndim> > right, NArySetOperation op) const;

protected:
    using typename CBLQSetOperations<ndim>::RegionEncodingCPtrCIter;
    using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;
};

template<int ndim>
class CBLQSetOperationsFast : public CBLQSetOperations<ndim> {
public:
	CBLQSetOperationsFast(CBLQSetOperationsConfig conf) : CBLQSetOperations<ndim>(conf) {}
	virtual ~CBLQSetOperationsFast() {}

private:
    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > unary_set_op_impl(boost::shared_ptr< const CBLQRegionEncoding<ndim> > region, UnarySetOperation op) const;
    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > binary_set_op_impl(boost::shared_ptr< const CBLQRegionEncoding<ndim> > left, boost::shared_ptr< const CBLQRegionEncoding<ndim> > right, NArySetOperation op) const;

    //virtual boost::shared_ptr< const CBLQRegionEncoding<ndim> > nary_set_op(RegionEncodingCPtrCIter region_it, RegionEncodingCPtrCIter region_end_it, NArySetOperation op) const;

protected:
    using typename CBLQSetOperations<ndim>::RegionEncodingCPtrCIter;
    using cblq_word_t = typename CBLQRegionEncoding<ndim>::cblq_word_t;
};

// Code-by-code nary set ops
template<int ndim>
class CBLQSetOperationsNAry1 : public CBLQSetOperations<ndim> {
public:
	CBLQSetOperationsNAry1(CBLQSetOperationsConfig conf) : CBLQSetOperations<ndim>(conf) {}
	virtual ~CBLQSetOperationsNAry1() {}

private:
	using typename CBLQSetOperations<ndim>::RegionEncodingCPtrCIter;
    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > nary_set_op_impl(RegionEncodingCPtrCIter region_it, RegionEncodingCPtrCIter region_end_it, NArySetOperation op) const;

private:
    typedef typename CBLQSetOperations<ndim>::cblq_word_t cblq_word_t;
};

// Word-by-word nary set ops with dense suffix support
template<int ndim>
class CBLQSetOperationsNAry2Dense : public CBLQSetOperations<ndim> {
public:
	CBLQSetOperationsNAry2Dense(CBLQSetOperationsConfig conf) : CBLQSetOperations<ndim>(conf) {}
	virtual ~CBLQSetOperationsNAry2Dense() {}

private:
	using typename CBLQSetOperations<ndim>::RegionEncodingCPtrCIter;
    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > nary_set_op_impl(RegionEncodingCPtrCIter region_it, RegionEncodingCPtrCIter region_end_it, NArySetOperation op) const;

private:
    typedef typename CBLQSetOperations<ndim>::cblq_word_t cblq_word_t;
    typedef typename CBLQRegionEncoding<ndim>::cblq_semiword_t cblq_semiword_t;
};

// Level-by-level nary set ops with dense suffix support
template<int ndim>
class CBLQSetOperationsNAry3Dense : public CBLQSetOperations<ndim> {
public:
	CBLQSetOperationsNAry3Dense(CBLQSetOperationsConfig conf) : CBLQSetOperations<ndim>(conf) {}
	virtual ~CBLQSetOperationsNAry3Dense() {}

private:
	using typename CBLQSetOperations<ndim>::RegionEncodingCPtrCIter;
    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > nary_set_op_impl(RegionEncodingCPtrCIter region_it, RegionEncodingCPtrCIter region_end_it, NArySetOperation op) const;

private:
    typedef typename CBLQSetOperations<ndim>::cblq_word_t cblq_word_t;
    typedef typename CBLQRegionEncoding<ndim>::cblq_semiword_t cblq_semiword_t;
};

// Level-by-level nary set ops with dense suffix support, optimized for union
template<int ndim>
class CBLQSetOperationsNAry3Fast : public CBLQSetOperationsFast<ndim> {
private:
    typedef typename CBLQRegionEncoding<ndim>::cblq_word_t cblq_word_t;
    typedef typename CBLQRegionEncoding<ndim>::cblq_semiword_t cblq_semiword_t;
    typedef typename CBLQSemiwords<ndim>::block_t semiword_block_t;

public:
	CBLQSetOperationsNAry3Fast(CBLQSetOperationsConfig conf) : CBLQSetOperationsFast<ndim>(conf) {}
	virtual ~CBLQSetOperationsNAry3Fast() {}

private:
	//virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > binary_set_op(boost::shared_ptr< const CBLQRegionEncoding<ndim> > left, boost::shared_ptr< const CBLQRegionEncoding<ndim> > right, NArySetOperation op) const;

	using typename CBLQSetOperations<ndim>::RegionEncodingCPtrCIter;
    virtual boost::shared_ptr< CBLQRegionEncoding<ndim> > nary_set_op_impl(RegionEncodingCPtrCIter region_it, RegionEncodingCPtrCIter region_end_it, NArySetOperation op) const;

    template<NArySetOperation op>
    inline boost::shared_ptr< CBLQRegionEncoding<ndim> > nary_set_op_impl(RegionEncodingCPtrCIter cblq_it, RegionEncodingCPtrCIter cblq_end_it) const;
};

#endif /* CBLQ_SETOPS_HPP_ */
