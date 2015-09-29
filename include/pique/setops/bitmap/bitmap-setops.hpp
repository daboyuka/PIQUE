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
 * bitmap-setops.hpp
 *
 *  Created on: Aug 8, 2014
 *      Author: David A. Boyuka II
 */
#ifndef BITMAP_SETOPS_HPP_
#define BITMAP_SETOPS_HPP_

#include <boost/smart_ptr.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/region/bitmap/bitmap.hpp"
#include "pique/setops/setops.hpp"

class BitmapSetOperationsConfig {};

class BitmapSetOperations : public SetOperations< BitmapRegionEncoding > {
public:
	typedef BitmapSetOperationsConfig SetOperationsConfig;
	BitmapSetOperations(BitmapSetOperationsConfig conf) : conf(conf) {}
	virtual ~BitmapSetOperations() {}

private:
	// Templated set operations
    virtual boost::shared_ptr< BitmapRegionEncoding > unary_set_op_impl(boost::shared_ptr< const BitmapRegionEncoding > region, UnarySetOperation op) const;
    virtual void inplace_unary_set_op_impl(boost::shared_ptr< BitmapRegionEncoding > region_and_out, UnarySetOperation op) const;

    virtual boost::shared_ptr< BitmapRegionEncoding > binary_set_op_impl(boost::shared_ptr< const BitmapRegionEncoding > left, boost::shared_ptr< const BitmapRegionEncoding > right, NArySetOperation op) const;
    virtual void inplace_binary_set_op_impl(boost::shared_ptr< BitmapRegionEncoding > left_and_out, boost::shared_ptr< const BitmapRegionEncoding > right, NArySetOperation op) const;

    // default nary_set_op_impl (heap/list-based binary expression tree)

    using typename SetOperations< BitmapRegionEncoding >::RegionEncodingCPtrCIter;
    virtual void inplace_nary_set_op_impl(boost::shared_ptr< BitmapRegionEncoding > first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const;

protected:
    const BitmapSetOperationsConfig conf;
};

#endif /* BITMAP_SETOPS_HPP_ */
