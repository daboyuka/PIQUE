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
 * query-engine.hpp
 *
 *  Created on: May 14, 2014
 *      Author: David A. Boyuka II
 */
#ifndef QUERY_ENGINE_HPP_
#define QUERY_ENGINE_HPP_

#include <vector>
#include <map>
#include <iostream>
#include <limits>
#include <boost/smart_ptr.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/indexing/binned-index-types.hpp"
#include "pique/io/index-io-types.hpp"
#include "pique/query/query.hpp"
#include "pique/stats/stats.hpp"
#include "pique/util/openclose.hpp"

struct QueryEngineOptions {
	enum struct ComplementMode { AUTO, ALWAYS, NEVER };

	QueryEngineOptions(ComplementMode complement_mode = ComplementMode::AUTO) :
		complement_mode(complement_mode)
	{}

	void dump(std::ostream &out = std::cout) const {
		out << "Complement bin merge mode: " << (complement_mode == ComplementMode::AUTO ? "auto" : complement_mode == ComplementMode::ALWAYS ? "always" : "never") << std::endl;
	}

	ComplementMode complement_mode;
};

class QueryEngine : public OpenableCloseable< boost::shared_ptr< Database > > {
public:
	using bin_id_t = BinnedIndexTypes::bin_id_t;
	using partition_id_t = IndexIOTypes::partition_id_t;
	using domain_id_t = IndexIOTypes::domain_id_t;
	using domain_t = IndexIOTypes::domain_t;

public:
	struct TermEvalStats {
		virtual ~TermEvalStats() {}
		std::string name;
		TimeStats total;
		uint64_t output_region_bytes;
	};
	struct ConstraintTermEvalStats : public TermEvalStats {
		virtual ~ConstraintTermEvalStats() {}
		bin_id_t lb_bin; // [lb_bin, ub_bin)
		bin_id_t ub_bin;

		bool forced_complement_binmerge_mode;
		bool used_complement_binmerge;
		uint64_t used_bincount;
		uint64_t used_binbytes;
		uint64_t other_bincount;
		uint64_t other_binbytes;

		IOStats binread;

		TimeStats binmerge;
		TimeStats binmerge_complement;
	};
	struct MultivarTermEvalStats : public TermEvalStats {
		virtual ~MultivarTermEvalStats() {}
		uint64_t arity;
		uint64_t in_regions_bytes;
	};
	struct QueryStats {
		TimeStats total;
		IOStats iototal;
		TimeStats decodetotal; // Index decoding for each constraint
		TimeStats setopstotal; // Combining of constraints into final result
		std::vector< boost::shared_ptr< TermEvalStats > > terminfos;
	};
	struct QuerySummaryStats : public BaseStats< QuerySummaryStats > {
		template<typename CombineFn> void combine(const QuerySummaryStats &other, CombineFn combine) {
			combine(this->total,         other.total        );
			combine(this->nary_term_ops, other.nary_term_ops);
			combine(this->bin_merge,     other.bin_merge    );
			combine(this->bin_read,      other.bin_read     );
		}

		void operator+=(const QueryStats &qstats);

		TimeStats total;
		TimeStats nary_term_ops;
		TimeStats bin_merge;
		TimeStats bin_read;
	};

	struct QueryPartitionResult {
		partition_id_t partition;
		domain_t partition_domain;
		QueryStats stats;
		boost::shared_ptr< RegionEncoding > result;
	};

	class QueryCursor {
	public:
		QueryCursor(domain_id_t cur_domain, domain_id_t end_domain, domain_id_t domain_step = 1) :
			cur_domain(cur_domain), end_domain(end_domain), domain_step(domain_step) {}
		virtual ~QueryCursor() {}

		bool has_next() { return cur_domain < end_domain && this->has_next_impl(); }
		QueryPartitionResult next() {
			if (!this->has_next()) abort();
			QueryPartitionResult result = this->next_impl();
			this->cur_domain += domain_step;
			return result;
		}

		domain_id_t get_current_domain_id() const { return cur_domain; }
		domain_id_t get_domain_id_step() const { return domain_step; }

	protected:
		void restrict_end_partition(domain_id_t new_end_domain) { this->end_domain = std::min(this->end_domain, new_end_domain); }

	private:
		virtual bool has_next_impl() { return true; }
		virtual QueryPartitionResult next_impl() = 0;

	private:
		domain_id_t cur_domain, end_domain, domain_step;
	};

public:
	QueryEngine(QueryEngineOptions options = QueryEngineOptions()) : options(options), database(nullptr) {}
	virtual ~QueryEngine() { this->close(); }

	boost::shared_ptr< RegionEncoding > evaluate(const Query &query, domain_id_t domain_id, QueryStats &qstats);
	boost::shared_ptr< QueryCursor > evaluate(const Query &query, domain_id_t begin_domain_id = 0, domain_id_t end_domain_id = std::numeric_limits< domain_id_t >::max());

	void reset_timings();
	QuerySummaryStats get_timings() const;

protected:
	virtual bool open_impl(boost::shared_ptr< Database > db);
	virtual bool close_impl();

private:
	virtual boost::shared_ptr< QueryCursor > evaluate_impl(const Query &query, domain_id_t begin_domain_id, domain_id_t end_domain_id) = 0;

protected:
	const QueryEngineOptions options;
	boost::shared_ptr< Database > database;

	QuerySummaryStats timings;
};

DEFINE_STATS_SERIALIZE(QueryEngine::QuerySummaryStats, stats, stats.total & stats.nary_term_ops & stats.bin_merge & stats.bin_read)

#endif /* QUERY_ENGINE_HPP_ */
