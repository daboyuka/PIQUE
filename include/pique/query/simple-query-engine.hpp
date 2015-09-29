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
 * simple-query-engine.hpp
 *
 *  Created on: Dec 3, 2014
 *      Author: David A. Boyuka II
 */
#ifndef SIMPLE_QUERY_ENGINE_HPP_
#define SIMPLE_QUERY_ENGINE_HPP_

#include <string>
#include <set>
#include <unordered_map>

#include "pique/encoding/region-math.hpp"
#include "pique/io/database.hpp"
#include "pique/io/index-io-cache.hpp"
#include "pique/query/query-engine.hpp"

// Use forward declaration rather than full include, for compile speed
//#include "pique/io/index-io.hpp"
class IndexIO;
class IndexPartitionIO;

// TODO: Implement safety against closing/reopening the query engine while QueryCursors from the old open session are still outstanding
class SimpleQueryEngine : public QueryEngine {
public:
	using domain_id_t = IndexIOTypes::domain_id_t;
	using partition_id_t = IndexIOTypes::partition_id_t;
public:
	// API implementation
	SimpleQueryEngine(QueryEngineOptions options = QueryEngineOptions()) : QueryEngine(std::move(options)), iocache(nullptr) {}
	~SimpleQueryEngine() { this->close(); }

	void clear_cache() { this->iocache->release_all(); }

protected:
	virtual bool open_impl(boost::shared_ptr< Database > db);
	virtual bool close_impl();

private:
	virtual boost::shared_ptr< QueryCursor > evaluate_impl(const Query &query, domain_id_t begin_domain_id, domain_id_t end_domain_id);

protected:
	using bin_id_t = BinnedIndexTypes::bin_id_t;
	using bin_id_range_t = std::pair< bin_id_t, bin_id_t >;
	using region_id_t = BinnedIndexTypes::region_id_t;
	using RegionMap = std::map< region_id_t, boost::shared_ptr< RegionEncoding > >;

	// Wrapper describing an invocation of evaluate_constraint that can be evaluated at a later time
	struct DeferredConstraintEvaluator {
		DeferredConstraintEvaluator(const ConstraintTerm &constr, const SimpleQueryEngine &qe) :
			constr(constr), qe(qe) {}

		boost::shared_ptr< RegionEncoding > operator()(partition_id_t part, ConstraintTermEvalStats &terminfo) const {
			return qe.evaluate_constraint_at_partition(constr, part, terminfo);
		}

		const ConstraintTerm &constr;
		const SimpleQueryEngine &qe;
	};

	using RegionEncodingPtrCIter = typename std::vector< boost::shared_ptr< RegionEncoding > >::const_iterator;
	using MutabilityCIter = typename std::vector< int >::const_iterator;

private:
	// Virtual delegate functions to be implemented by derived classes

	// Perform query plan optimization on (default implementation: do nothing)
	virtual void optimize_constraint_region_math(IndexPartitionIO &partio, RegionMath::RegionMath &rmath) const {}
	virtual void optimize_query_region_math(const std::vector< DeferredConstraintEvaluator > &constraints, const std::vector< partition_id_t > &constraint_partitions, RegionMath::RegionMath &rmath) const {}

	// Estimates the cost of evaluating a given RegionMath, including I/O and setops cost
	virtual uint64_t compute_constraint_evaluation_cost(IndexPartitionIO &partio, const RegionMath::RegionMath &rmath) const;

	virtual boost::shared_ptr< RegionEncoding > evaluate_set_op(boost::shared_ptr< RegionEncoding > region, bool is_region_mutable, UnarySetOperation op) const = 0;
	virtual boost::shared_ptr< RegionEncoding > evaluate_set_op(RegionEncodingPtrCIter begin, RegionEncodingPtrCIter end, MutabilityCIter mutability_begin, MutabilityCIter mutability_end, NArySetOperation op) const = 0;

protected:
	// Helper functions for use by derived classes (and this class)

	// Uses BinningSpecification quantization to convert bound values to bound bins
	bin_id_range_t compute_bin_range(IndexPartitionIO &partio, const ConstraintTerm &constraint) const;

	// Uses IndexEncoding to produce two alternate evaluation plans, complement
	// and no-complement, and chooses the one with lesser cost (based on the
	// cost metric of this->compute_constraint_evaluation_cost()).
	RegionMath::RegionMath compute_optimal_region_math_for_bin_range(IndexPartitionIO &partio, bin_id_range_t bin_range, ConstraintTermEvalStats &terminfo) const;

private:
	// Implementation helper functions

	// Evaluates a single constraint to RegionEncoding
	boost::shared_ptr< RegionEncoding > evaluate_constraint_at_partition(const ConstraintTerm &cqt, partition_id_t partition, ConstraintTermEvalStats &terminfo) const;

	// Converts a Query into equivalent RegionMath and DeferredConstraintEvaluators
	void convert_query_to_region_math(const Query &query, std::vector< DeferredConstraintEvaluator > &constraints, RegionMath::RegionMath &rmath) const;

	// Evaluates region math using a stack and the underlying evaluate_set_op virtual delegate functions
	boost::shared_ptr< RegionEncoding > evaluate_query_region_math(
			std::vector< DeferredConstraintEvaluator > &constraints,
			std::vector< partition_id_t > &constraint_partitions,
			const RegionMath::RegionMath &rmath,
			QueryStats &qstats) const;

	boost::shared_ptr< RegionEncoding > evaluate_constraint_region_math(
			RegionMap &rmap,
			const RegionMath::RegionMath &rmath,
			ConstraintTermEvalStats &terminfo) const;

private:
	boost::shared_ptr< IndexIOCache > iocache;

private:
	friend class EvaluateVisitor;
	friend class SimpleQueryCursor;
};

#endif /* SIMPLE_QUERY_ENGINE_HPP_ */
