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
 * ii-setops.hpp
 *
 *  Created on: Mar 19, 2014
 *      Author: David A. Boyuka II
 */
#ifndef II_SETOPS_HPP_
#define II_SETOPS_HPP_

#include <boost/smart_ptr.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/ii/ii.hpp"
#include "pique/setops/setops.hpp"

class IISetOperationsConfig {};

class IISetOperations : public SetOperations< IIRegionEncoding > {
public:
	typedef IISetOperationsConfig SetOperationsConfig;
	IISetOperations(IISetOperationsConfig conf) : conf(conf) {}
	virtual ~IISetOperations() {}

private:
    virtual boost::shared_ptr< IIRegionEncoding > unary_set_op_impl(boost::shared_ptr< const IIRegionEncoding > region, UnarySetOperation op) const;
    virtual boost::shared_ptr< IIRegionEncoding > binary_set_op_impl(boost::shared_ptr< const IIRegionEncoding > left, boost::shared_ptr< const IIRegionEncoding > right, NArySetOperation op) const;

protected:
    typedef IIRegionEncoding::rid_t rid_t;

protected:
    const IISetOperationsConfig conf;
};

class IISetOperationsNAry : public IISetOperations {
public:
	IISetOperationsNAry(IISetOperationsConfig conf) : IISetOperations(conf) {}

protected:
	using typename SetOperations< IIRegionEncoding >::RegionEncodingCPtrCIter;
    virtual boost::shared_ptr< IIRegionEncoding > nary_set_op_impl(RegionEncodingCPtrCIter region_it, RegionEncodingCPtrCIter region_end_it, NArySetOperation op) const;
};

#endif /* II_SETOPS_HPP_ */
