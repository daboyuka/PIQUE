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
 * precision.hpp
 *
 *  Created on: Dec 4, 2014
 *      Author: David A. Boyuka II
 */
#ifndef PRECISION_HPP_
#define PRECISION_HPP_

#include <stdlib.h>
#include <limits>	// std::numeric_limits
#include <float.h>
#include <math.h>	// fabs, floor, ceil, log10, ...

// COPIED FROM FastBit/src/util.h, RENAMED TO coarsen_double

/// In an attempt to compute small values more consistently, small values
/// are computed through division of integer values.  Since these integer
/// values are computed through the function pow, the accuracy of the
/// results depend on the implementation of the math library.
///
/// The value zero is always rounded to zero.   Incoming value less than
/// 1E-300 or greater than 1E300 is rounded to zero.
namespace ibis { namespace util {
inline double coarsen_double(const double in, unsigned int prec) {
    double ret;
    if (prec > 15) {
	ret = in;
    }
    else if (in == 0.0) {
	ret = in;
    }
    else {
	ret = fabs(in);
	if (ret < DBL_MIN) { // denormalized number --> 0
	    ret = 0.0;
	}
	else if (ret < DBL_MAX) { // normal numbers
	    ret = log10(ret);
	    if (prec > 0)
		-- prec;
	    const int ixp = static_cast<int>(floor(ret)) -
		static_cast<int>(prec);
	    ret = floor(0.5 + pow(1e1, ret-ixp));
	    if (ixp > 0)
		ret *= pow(1e1, ixp);
	    else if (ixp < 0)
		ret /= pow(1e1, -ixp);
	    if (in < 0.0)
		ret = -ret;
	}
	else {
	    ret = in;
	}
    }
    return ret;
} // ibis::util::coarsen_double
}} // namespace

#endif /* PRECISION_HPP_ */
