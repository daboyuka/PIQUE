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
 * query-engine.cpp
 *
 *  Created on: May 14, 2014
 *      Author: David A. Boyuka II
 */

#include <typeinfo>
#include <iostream>
#include <cassert>
#include <iterator>
#include <set>

#include <boost/smart_ptr.hpp>
#include <boost/iterator/counting_iterator.hpp>

#include "pique/query/query-engine.hpp"
#include "pique/indexing/quantization.hpp"
#include "pique/util/dynamic-dispatch.hpp"
#include "pique/util/timing.hpp"

void QueryEngine::QuerySummaryStats::operator+=(const QueryStats& qstats) {
	using TermEvalStats = QueryEngine::TermEvalStats;
	using ConstraintStats = QueryEngine::ConstraintTermEvalStats;
	using MultivarStats = QueryEngine::MultivarTermEvalStats;

	for (boost::shared_ptr< TermEvalStats > termstats : qstats.terminfos) {
		const std::type_info &ti = typeid(*termstats);
		if (ti == typeid(ConstraintStats)) {
			const ConstraintStats &cs = static_cast< ConstraintStats & >(*termstats);

			this->bin_merge += cs.binmerge;
			this->bin_merge += cs.binmerge_complement;
			this->bin_read += cs.binread.get_read_as_time_stats();
			this->total += cs.total;
		} else if (ti == typeid(MultivarStats)) {
			const MultivarStats &ms = static_cast< MultivarStats & >(*termstats);

			this->nary_term_ops += ms.total;
		} else {
			std::cerr << "Warning: unknown TermEvalStats type at " << __FILE__ << ":" << __LINE__ << std::endl;
		}
	}
}

boost::shared_ptr< RegionEncoding > QueryEngine::evaluate(const Query& query, domain_id_t domain_id, QueryStats& qstats) {
	boost::shared_ptr< QueryCursor > cursor = this->evaluate(query, domain_id, domain_id + 1);
	assert(cursor->has_next());

	QueryPartitionResult result = cursor->next();
	qstats = result.stats;
	return result.result;
}

auto QueryEngine::evaluate(const Query& query, domain_id_t begin_domain_id, domain_id_t end_domain_id) -> boost::shared_ptr< QueryCursor > {
	return this->evaluate_impl(query, begin_domain_id, end_domain_id);
}

bool QueryEngine::open_impl(boost::shared_ptr<Database> db) {
	this->database = db;
	return true;
}

bool QueryEngine::close_impl() {
	this->database = nullptr;
	return true;
}

void QueryEngine::reset_timings() {
	timings = QuerySummaryStats();
}

auto QueryEngine::get_timings() const -> QuerySummaryStats  {
	return QuerySummaryStats(timings);
}
