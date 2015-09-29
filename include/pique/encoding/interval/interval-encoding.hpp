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
 * interval-encoding.hpp
 *
 *  Created on: Dec 2, 2014
 *      Author: David A. Boyuka II
 */
#ifndef INTERVAL_ENCODING_HPP_
#define INTERVAL_ENCODING_HPP_

#include <boost/serialization/access.hpp>

#include "pique/encoding/index-encoding.hpp"

class IntervalIndexEncoding : public IndexEncoding {
public:
	IntervalIndexEncoding() : IndexEncoding(Type::INTERVAL) {}

private:
	virtual std::vector< bin_id_vector_t > get_encoded_region_definitions_impl(bin_count_t nbins) const;
	virtual region_vector_t get_encoded_regions_impl(region_vector_t bins, const AbstractSetOperations &setops) const;

	virtual RMath get_region_math_impl(bin_count_t nbins, bin_id_t lb, bin_id_t ub, bool prefer_complement) const; // [lb, ub)

private:
	friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {}
};



#endif /* INTERVAL_ENCODING_HPP_ */
