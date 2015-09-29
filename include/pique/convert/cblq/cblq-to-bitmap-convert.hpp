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
 * cblq-to-bitmap-convert.hpp
 *
 *  Created on: Jun 17, 2014
 *      Author: David A. Boyuka II
 */
#ifndef CBLQ_TO_BITMAP_CONVERT_HPP_
#define CBLQ_TO_BITMAP_CONVERT_HPP_

#include <boost/smart_ptr.hpp>

#include "pique/region/cblq/cblq.hpp"
#include "pique/region/bitmap/bitmap.hpp"
#include "pique/convert/region-convert.hpp"

template<int ndim>
class CBLQToBitmapConverter : public RegionEncodingConverter< CBLQRegionEncoding<ndim>, BitmapRegionEncoding > {
public:
	virtual ~CBLQToBitmapConverter() {}

	virtual boost::shared_ptr< BitmapRegionEncoding > convert(boost::shared_ptr< const CBLQRegionEncoding<ndim> > in) const;
	virtual void inplace_convert(boost::shared_ptr< const CBLQRegionEncoding<ndim> > in_right, boost::shared_ptr< BitmapRegionEncoding > out_combine_left, NArySetOperation combine_op) const;

private:
	template<NArySetOperation combineop>
	void inplace_convert(boost::shared_ptr< const CBLQRegionEncoding<ndim> > in_right, boost::shared_ptr< BitmapRegionEncoding > out_combine_left) const;
};


template<int ndim>
class CBLQToBitmapDFConverter : public RegionEncodingConverter< CBLQRegionEncoding<ndim>, BitmapRegionEncoding > {
public:
	virtual ~CBLQToBitmapDFConverter() {}

	virtual boost::shared_ptr< BitmapRegionEncoding > convert(boost::shared_ptr< const CBLQRegionEncoding<ndim> > in) const;
	virtual void inplace_convert(boost::shared_ptr< const CBLQRegionEncoding<ndim> > in_right, boost::shared_ptr< BitmapRegionEncoding > out_combine_left, NArySetOperation combine_op) const;

private:
	template<NArySetOperation combineop>
	void inplace_convert(boost::shared_ptr< const CBLQRegionEncoding<ndim> > in_right, boost::shared_ptr< BitmapRegionEncoding > out_combine_left) const;
};

#endif /* CBLQ_TO_BITMAP_CONVERT_HPP_ */
