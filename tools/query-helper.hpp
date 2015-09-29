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
 * query-helper.hpp
 *
 *  Created on: May 21, 2014
 *      Author: David A. Boyuka II
 */
#ifndef QUERY_HELPER_HPP_
#define QUERY_HELPER_HPP_

#include <string>

#include <boost/smart_ptr.hpp>

#include <pique/query/query-engine.hpp>

boost::shared_ptr< QueryEngine > configure_query_engine(
		std::string qe_mode = std::string(), std::string setops_mode = std::string(), std::string convert_mode = std::string(),
		QueryEngineOptions options = QueryEngineOptions(), bool verbose = false);

#endif /* QUERY_HELPER_HPP_ */
