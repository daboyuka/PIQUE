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
 * grid-subset.hpp
 *
 *  Created on: Sep 2, 2014
 *      Author: David A. Boyuka II
 */
#ifndef GRID_SUBSET_HPP_
#define GRID_SUBSET_HPP_

#include <vector>

#include "pique/data/grid.hpp"

class GridSubset {
public:
	using dataset_offset_t = uint64_t;
	using dataset_length_t = dataset_offset_t;

	enum struct Type { WHOLE_DOMAIN, LINEARIZED_RANGES, SUBVOLUME };

	GridSubset(Grid domain_grid) : type(Type::WHOLE_DOMAIN), domain_grid(domain_grid) {}

	GridSubset(Grid domain_grid, dataset_offset_t start, dataset_length_t len) :
		type(Type::LINEARIZED_RANGES), domain_grid(domain_grid), ranges(1, std::make_pair<>(start, len))
	{}
	GridSubset(Grid domain_grid, std::vector< std::pair< dataset_offset_t, dataset_length_t > > ranges) :
		type(Type::LINEARIZED_RANGES), domain_grid(domain_grid), ranges(std::move(ranges))
	{}
	GridSubset(Grid domain_grid, std::vector< uint64_t > subvolume_offsets, std::vector< uint64_t > subvolume_dims) :
		type(Type::SUBVOLUME), domain_grid(domain_grid), subvolume_offsets(std::move(subvolume_offsets)), subvolume_dims(std::move(subvolume_dims))
	{}

	Type get_type() const { return type; }
	Grid get_grid() const { return domain_grid; }
	const std::vector< std::pair< dataset_offset_t, dataset_length_t > > & get_ranges() const { return ranges; }
	const std::vector< uint64_t > & get_subvolume_offsets() const { return subvolume_offsets; }
	const std::vector< uint64_t > & get_subvolume_dims() const { return subvolume_dims; }

private:
	const Type type;
	const Grid domain_grid;
	const std::vector< std::pair< dataset_offset_t, dataset_length_t > > ranges;
	const std::vector< uint64_t > subvolume_offsets, subvolume_dims;
};

#endif /* GRID_SUBSET_HPP_ */
