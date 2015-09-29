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
 * interval.hpp
 *
 *  Created on: Jun 23, 2014
 *      Author: David A. Boyuka II
 */
#ifndef INTERVAL_HPP_
#define INTERVAL_HPP_

template<typename BoundT>
struct Interval {
	struct Bound {
		Bound(BoundT bound, bool closed, bool infinite) :
			bound(bound), closed(closed), infinite(infinite)
		{}

		const BoundT bound;
		const bool closed, infinite;
	};

	Interval(BoundT lb, BoundT ub, bool lb_closed = true, bool ub_closed = false, bool lb_infinite = false, bool ub_infinite = false) :
		lb(lb, lb_closed, lb_infinite), ub(ub, ub_closed, ub_infinite)
	{}

	const Bound lb, ub;
};

#endif /* INTERVAL_HPP_ */
