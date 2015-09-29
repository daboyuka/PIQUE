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
 * grid.hpp
 *
 *  Created on: Jan 24, 2014
 *      Author: David A. Boyuka II
 */

#ifndef GRID_HPP_
#define GRID_HPP_

#include <boost/smart_ptr.hpp>

class Grid : public std::vector< uint64_t > {
public:
    enum class Linearization { ROW_MAJOR_ORDER, Z_ORDER, MORTON_ORDER = Z_ORDER /* alias */, };

public:
    using ParentType = std::vector< uint64_t >;
    Grid(Linearization lin = Linearization::ROW_MAJOR_ORDER) : ParentType(), lin(lin) {}
    Grid(std::vector< uint64_t > &&dims, Linearization lin = Linearization::ROW_MAJOR_ORDER) : ParentType(std::move(dims)), lin(lin) {}
    Grid(std::initializer_list< uint64_t > &&dims) : ParentType(std::move(dims)), lin(Linearization::ROW_MAJOR_ORDER) {}

    Linearization get_linearization() const { return lin; }

    uint64_t get_npoints() const {
    	uint64_t count = 1;
    	for (uint64_t dim : *this)
    		count *= dim;
    	return count;
    }

private:
    Linearization lin;
};

#endif /* GRID_HPP_ */
