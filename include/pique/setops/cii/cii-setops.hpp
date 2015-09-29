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
 * cii-setops.hpp
 *
 *  Created on: Mar 22, 2014
 *      Author: David A. Boyuka II
 */
#ifndef CII_SETOPS_HPP_
#define CII_SETOPS_HPP_

#include <boost/smart_ptr.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/cii/cii.hpp"
#include "pique/setops/setops.hpp"

struct CIISetOperationsConfig {
	CIISetOperationsConfig(bool compress_after = false) :
		compress_after(compress_after)
	{}

	bool compress_after;
};

class CIISetOperations : public SetOperations< CIIRegionEncoding > {
public:
	typedef CIISetOperationsConfig SetOperationsConfig;
	CIISetOperations(CIISetOperationsConfig conf) : conf(conf) {}
	virtual ~CIISetOperations() {}

protected:
    using typename SetOperations< CIIRegionEncoding >::RegionEncodingCPtrCIter;
    typedef CIIRegionEncoding::rid_t rid_t;

private:
    virtual boost::shared_ptr< CIIRegionEncoding > unary_set_op_impl(boost::shared_ptr< const CIIRegionEncoding > region, UnarySetOperation op) const;
    virtual boost::shared_ptr< CIIRegionEncoding > binary_set_op_impl(boost::shared_ptr< const CIIRegionEncoding > left, boost::shared_ptr< const CIIRegionEncoding > right, NArySetOperation op) const;

    // Same as standard SetOperations::nary_set_op, but uses smarter region size comparison
    virtual boost::shared_ptr< CIIRegionEncoding > nary_set_op_impl(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const;

protected:
    boost::shared_ptr< CIIRegionEncoding > nary_set_op_smartcompare_impl(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const;

    boost::shared_ptr< CIIRegionEncoding > binary_set_op_mixed(boost::shared_ptr< const CIIRegionEncoding > left, boost::shared_ptr< const CIIRegionEncoding > right, NArySetOperation op) const;
    boost::shared_ptr< CIIRegionEncoding > binary_set_op_uncompressed(boost::shared_ptr< const CIIRegionEncoding > left, boost::shared_ptr< const CIIRegionEncoding > right, NArySetOperation op) const;

protected:
    const CIISetOperationsConfig conf;
};

// Adds smart N-ary set ops
class CIISetOperationsNAry : public CIISetOperations {
public:
	CIISetOperationsNAry(CIISetOperationsConfig conf) : CIISetOperations(conf) {}

protected:
	using CIISetOperations::RegionEncodingCPtrCIter;
    virtual boost::shared_ptr< CIIRegionEncoding > nary_set_op_impl(RegionEncodingCPtrCIter region_it, RegionEncodingCPtrCIter region_end_it, NArySetOperation op) const;
};

#endif /* CII_SETOPS_HPP_ */
