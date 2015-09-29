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
 * basic-query-engine.hpp
 *
 *  Created on: Aug 7, 2014
 *      Author: David A. Boyuka II
 */
#ifndef BASIC_QUERY_ENGINE_HPP_
#define BASIC_QUERY_ENGINE_HPP_

#include "pique/setops/setops.hpp"
#include "pique/query/simple-query-engine.hpp"

class BasicQueryEngine : public SimpleQueryEngine {
public:
	using QueryEngine::QueryStats;
	using QueryEngine::QuerySummaryStats;

	BasicQueryEngine(boost::shared_ptr< AbstractSetOperations > setops, QueryEngineOptions options = QueryEngineOptions()) :
		SimpleQueryEngine(options), setops(setops) {}
	virtual ~BasicQueryEngine() { this->close(); }

private:
	virtual boost::shared_ptr< RegionEncoding > evaluate_set_op(boost::shared_ptr< RegionEncoding > region, bool is_region_mutable, UnarySetOperation op) const;
	virtual boost::shared_ptr< RegionEncoding > evaluate_set_op(RegionEncodingPtrCIter begin, RegionEncodingPtrCIter end, MutabilityCIter mutability_begin, MutabilityCIter mutability_end, NArySetOperation op) const;

private:
	boost::shared_ptr< AbstractSetOperations > setops;
};

#endif /* BASIC_QUERY_ENGINE_HPP_ */
