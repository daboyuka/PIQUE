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
 * simple-query-engine.cpp
 *
 *  Created on: Dec 3, 2014
 *      Author: David A. Boyuka II
 */

#include <cassert>

#include "pique/encoding/index-encoding.hpp"
#include "pique/io/index-io.hpp"
#include "pique/query/simple-query-engine.hpp"
#include "pique/stats/stats.hpp"
#include "pique/util/timing.hpp"

// Simple RegionMath evaluator
struct EvaluateVisitor {
	using region_id_t = SimpleQueryEngine::region_id_t;
	using RegionMap = std::map< region_id_t, boost::shared_ptr< RegionEncoding > >;

	typedef void result_type;
	EvaluateVisitor(const SimpleQueryEngine &qe) : qe(qe) {}
	virtual ~EvaluateVisitor() {}

	void operator()(const RegionMath::RegionTerm &region) {
		stack.push_back(obtain_region(region.rid));
		stack_mut.push_back(false);
	}
	void operator()(const RegionMath::UnaryOperatorTerm &op) {
		stack.back() = qe.evaluate_set_op(stack.back(), stack_mut.back(), op.op);
		stack_mut.back() = true;
	}
	void operator()(const RegionMath::NAryOperatorTerm &op) {
		auto first_operand = stack.end() - op.arity;
		auto first_operand_mut = stack_mut.end() - op.arity;
		*first_operand = qe.evaluate_set_op(first_operand, stack.end(), first_operand_mut, stack_mut.end(), op.op);
		*first_operand_mut = true;
		stack.erase(first_operand + 1, stack.end());
		stack_mut.erase(first_operand_mut + 1, stack_mut.end());
	}

	virtual boost::shared_ptr< RegionEncoding > obtain_region(region_id_t region) = 0;

	const SimpleQueryEngine &qe;
	std::vector< boost::shared_ptr< RegionEncoding > > stack;
	std::vector< int > stack_mut;
};

class SimpleQueryCursor : public QueryEngine::QueryCursor {
public:
	using DFE = SimpleQueryEngine::DeferredConstraintEvaluator;
	using partition_id_t = IndexIOTypes::partition_id_t;
	using partition_count_t = IndexIOTypes::partition_count_t;
	using domain_t = IndexIOTypes::domain_t;
	using domain_id_t = IndexIOTypes::domain_id_t;
	using domain_mapping_t = IndexIOTypes::domain_mapping_t;

	virtual ~SimpleQueryCursor() {}
	SimpleQueryCursor(
			SimpleQueryEngine &sqe, std::vector< DFE > constraints, RegionMath::RegionMath multivar_rmath,
			domain_id_t cur_part, domain_id_t end_part);

	// Move-only, no copying
	SimpleQueryCursor(const SimpleQueryCursor &) = delete;
	SimpleQueryCursor(SimpleQueryCursor &&) = default;

private:
	virtual QueryEngine::QueryPartitionResult next_impl();

private:
	std::vector< std::vector< domain_mapping_t > > compute_partition_mapping() const;
	void compute_constraint_partitions(domain_id_t domain_id, std::vector< partition_id_t > &constraint_partitions, domain_t &domain) const;

private:
	SimpleQueryEngine &sqe;
	std::vector< DFE > constraints;
	RegionMath::RegionMath multivar_rmath;

	std::vector< std::vector< domain_mapping_t > > constraint_domain_mapping; // [constraint_id][sorted_partition_id] -> partition_id
};

SimpleQueryCursor::SimpleQueryCursor(
		SimpleQueryEngine &sqe, std::vector< DFE > constraints, RegionMath::RegionMath multivar_rmath,
		domain_id_t cur_part, domain_id_t end_part) :
	QueryCursor(cur_part, end_part),
	sqe(sqe), constraints(std::move(constraints)), multivar_rmath(std::move(multivar_rmath))
{
	this->constraint_domain_mapping = compute_partition_mapping();

	// Restrict the end of this cursor's range to the number of partitions that actually exist
	this->restrict_end_partition(this->constraint_domain_mapping[0].size());
}

auto SimpleQueryCursor::compute_partition_mapping() const -> std::vector< std::vector< domain_mapping_t > > {
	using domain_t = IndexIOTypes::domain_t;
	using domain_mapping_t = IndexIOTypes::domain_mapping_t;
	if (this->constraints.size() == 0)
		return std::vector< std::vector< domain_mapping_t > >();

	IndexIOCache &iocache = *this->sqe.iocache;
	std::vector< partition_count_t > constraint_part_counts;
	std::vector< std::vector< domain_mapping_t > > constraint_part_domain_mappings;

	for (auto &constraint : this->constraints) {
		boost::shared_ptr< IndexIO > iio = iocache.open_index_io(constraint.constr.varname);
		constraint_part_counts.push_back(iio->get_num_partitions());
		constraint_part_domain_mappings.push_back(iio->get_sorted_partition_domain_mappings());
	}

	const partition_count_t expected_part_count = constraint_part_counts[0];
	const std::vector< domain_mapping_t > &expected_part_domain_mappings = constraint_part_domain_mappings[0];

	// Ensure all constraints have the same partition count
	assert(std::all_of(
		constraint_part_counts.begin() + 1, constraint_part_counts.end(),
		[&](partition_count_t cur_num_parts)->bool{ return cur_num_parts == expected_part_count; })
	);

	// Ensure all partition domains are equal (implies in the same order)
	assert(std::all_of(
		constraint_part_domain_mappings.begin() + 1, constraint_part_domain_mappings.end(),
		[&](const std::vector< domain_mapping_t > &part_domain_mappings)->bool {
			return std::equal(
				part_domain_mappings.begin(), part_domain_mappings.end(),
				expected_part_domain_mappings.begin(),
				[](const domain_mapping_t &left, const domain_mapping_t &right)->bool {
					return left.second == right.second;
				}
			);
		}
	));

	return constraint_part_domain_mappings;
}

void SimpleQueryCursor::compute_constraint_partitions(domain_id_t domain_id, std::vector< partition_id_t > &constraint_partitions, domain_t &domain) const
{
	constraint_partitions.clear();

	// Retrieve the domain from the first constraint (all constraints have been verified to have equivalent domain partitioning)
	domain = this->constraint_domain_mapping[0][domain_id].second;

	// Convert each sorted partition ID -> true partition ID mapping table to the true partition ID at the requested sorted_partition
	std::transform(
		this->constraint_domain_mapping.begin(), this->constraint_domain_mapping.end(), std::back_inserter(constraint_partitions),
		[&](const std::vector< domain_mapping_t > &part_mapping) -> partition_id_t {
			return part_mapping[domain_id].first;
		}
	);
}

QueryEngine::QueryPartitionResult SimpleQueryCursor::next_impl() {
	assert(this->sqe.is_open());

	QueryEngine::QueryPartitionResult result;
	const domain_id_t domain_id = this->get_current_domain_id();

	TIME_STATS_TIME_BEGIN(result.stats.total)
	// Step 1: Compute partition IDs from the domain ID (also determine the domain bounds)
	domain_t domain;
	std::vector< partition_id_t > constraint_partitions; // For each constraint's index, which partition_id_t corresponds to the current domain_id_t
	this->compute_constraint_partitions(domain_id, constraint_partitions, domain);

	// Step 2: optimized the RegionMath for this particular partition
	RegionMath::RegionMath optimized_multivar_rmath = this->multivar_rmath;
	this->sqe.optimize_query_region_math(this->constraints, constraint_partitions, optimized_multivar_rmath);

	// Step 3: evaluate the query RegionMath
	result.partition = domain_id;
	result.partition_domain = domain;
	result.result = this->sqe.evaluate_query_region_math(this->constraints, constraint_partitions, optimized_multivar_rmath, result.stats);
	TIME_STATS_TIME_END()

	return result;
}


bool SimpleQueryEngine::open_impl(boost::shared_ptr< Database > db) {
	if (!this->QueryEngine::open_impl(db)) return false;
	this->iocache = boost::make_shared< IndexIOCache >(db, IndexOpenMode::READ);
	return true;
}

bool SimpleQueryEngine::close_impl() {
	this->iocache = nullptr;
	if (!this->QueryEngine::close_impl()) return false;
	return true;
}

auto SimpleQueryEngine::evaluate_impl(const Query &query, domain_id_t begin_domain_id, domain_id_t end_domain_id) -> boost::shared_ptr< QueryCursor >
{
	assert(this->is_open());

	RegionMath::RegionMath multivar_rmath;
	std::vector< DeferredConstraintEvaluator > constraints;

	// Compute the initial inter-variable query RegionMath
	this->convert_query_to_region_math(query, constraints, multivar_rmath);

	// Return a cursor, with query processing deferred until it is advanced
	return boost::make_shared< SimpleQueryCursor >(*this, std::move(constraints), std::move(multivar_rmath), begin_domain_id, end_domain_id);
}

void SimpleQueryEngine::convert_query_to_region_math(const Query& query, std::vector< DeferredConstraintEvaluator > &constraints, RegionMath::RegionMath &rmath) const {
	constraints.clear();
	rmath.clear();

	for (auto term_it = query.cbegin(); term_it != query.cend(); term_it++) {
		const QueryTerm &qt = **term_it;
		const std::type_info &ti = typeid(qt);

		if (ti == typeid(ConstraintTerm)) {
			const ConstraintTerm &cqt = dynamic_cast<const ConstraintTerm&>(qt);
			const region_id_t region_id = constraints.size(); // Get the next available region ID

			constraints.push_back(DeferredConstraintEvaluator(cqt, *this));
			rmath.push_region(region_id);
		} else if (ti == typeid(UnaryOperatorTerm)) {
			const UnaryOperatorTerm &uoqt = dynamic_cast<const UnaryOperatorTerm&>(qt);
			rmath.push_op(uoqt.op);
		} else if (ti == typeid(NAryOperatorTerm)) {
			const NAryOperatorTerm &noqt = dynamic_cast<const NAryOperatorTerm&>(qt);
			rmath.push_op(noqt.op, noqt.arity);
		}
	}
}

boost::shared_ptr< RegionEncoding > SimpleQueryEngine::evaluate_query_region_math(
		std::vector< DeferredConstraintEvaluator > &constraints,
		std::vector< partition_id_t > &constraint_partitions,
		const RegionMath::RegionMath &rmath,
		QueryStats &qstats) const
{
	class EvaluateQueryVisitor : public EvaluateVisitor {
	private:
		using ConstrTermStats = QueryEngine::ConstraintTermEvalStats;
		using MVTermStats = QueryEngine::MultivarTermEvalStats;

	public:
		typedef void result_type;
		EvaluateQueryVisitor(
				const SimpleQueryEngine &qe, const std::vector< DeferredConstraintEvaluator > &constraints,
				std::vector< partition_id_t > &constraint_partitions, QueryEngine::QueryStats &qs) :
			EvaluateVisitor(qe), constraints(constraints), constraint_partitions(constraint_partitions), qs(qs)
		{}

		void operator()(const RegionMath::RegionTerm &op) { this->EvaluateVisitor::operator()(op); }
		void operator()(const RegionMath::UnaryOperatorTerm &op) {
			MVTermStats &terminfo = new_multivar_term_stats();
			terminfo.arity = 1;
			terminfo.name = std::string("unary:") + (char)('0' + (char)op.op);
			terminfo.in_regions_bytes = stack.back()->get_size_in_bytes();
			TIME_STATS_TIME_BEGIN(terminfo.total);
			this->EvaluateVisitor::operator()(op);
			TIME_STATS_TIME_END();
			terminfo.output_region_bytes = stack.back()->get_size_in_bytes();
			qs.setopstotal += terminfo.total;
		}
		void operator()(const RegionMath::NAryOperatorTerm &op) {
			MVTermStats &terminfo = new_multivar_term_stats();
			terminfo.arity = op.arity;
			terminfo.name = std::string("nary:") + (char)('0' + (char)op.op);
			terminfo.in_regions_bytes = stack.back()->get_size_in_bytes();

			auto first_operand = stack.end() - op.arity, end_operands = stack.end();
			terminfo.in_regions_bytes = 0;
			for (auto it = first_operand; it != end_operands; ++it)
				terminfo.in_regions_bytes += (*it)->get_size_in_bytes();

			TIME_STATS_TIME_BEGIN(terminfo.total);
			this->EvaluateVisitor::operator()(op);
			TIME_STATS_TIME_END();
			terminfo.output_region_bytes = stack.back()->get_size_in_bytes();
			qs.setopstotal += terminfo.total;
		}

		virtual boost::shared_ptr< RegionEncoding > obtain_region(region_id_t constraint_id) {
			ConstrTermStats &terminfo = new_constraint_term_stats();
			const partition_id_t constraint_part = constraint_partitions[constraint_id];
			boost::shared_ptr< RegionEncoding > result = constraints[constraint_id](constraint_part, terminfo); // Evaluate the constraint on-the-fly
			qs.iototal += terminfo.binread;
			qs.decodetotal += terminfo.binmerge;
			return result;
		}

	private:
		ConstrTermStats & new_constraint_term_stats() {
			boost::shared_ptr< ConstrTermStats > stats = boost::make_shared< ConstrTermStats >();
			qs.terminfos.push_back(stats);
			return *stats;
		}
		MVTermStats & new_multivar_term_stats() {
			boost::shared_ptr< MVTermStats > stats = boost::make_shared< MVTermStats >();
			qs.terminfos.push_back(stats);
			return *stats;
		}

	private:
		const std::vector< DeferredConstraintEvaluator > &constraints;
		const std::vector< partition_id_t > &constraint_partitions;
		QueryEngine::QueryStats &qs;
	};

	boost::shared_ptr< RegionEncoding > result;

	// Evaluate (and time) the query as captured in the supplied RegionMath/RegionMap
	EvaluateQueryVisitor visitor(*this, constraints, constraint_partitions, qstats);
	for (const RegionMath::TermVariant &term : rmath)
		boost::apply_visitor(visitor, term);

	assert(visitor.stack.size() == 1);
	result = visitor.stack.back();

	return result;
}

boost::shared_ptr< RegionEncoding > SimpleQueryEngine::evaluate_constraint_at_partition(const ConstraintTerm &cqt, partition_id_t partition, ConstraintTermEvalStats &terminfo) const {
	TIME_STATS_TIME_BEGIN(terminfo.total)
	terminfo.name = cqt.varname;

	boost::shared_ptr< IndexPartitionIO > partio = this->iocache->open_index_partition_io(cqt.varname, partition);
	assert(partio);

	const bin_id_range_t bin_range = this->compute_bin_range(*partio, cqt);
	terminfo.lb_bin = bin_range.first;
	terminfo.ub_bin = bin_range.second;

	IndexPartitionIO::PartitionMetadata pmeta = partio->get_partition_metadata();

	// If the bin range is empty or completely covering, return an empty/full region immediately
	if (bin_range.first >= bin_range.second)
		return RegionEncoding::make_uniform_region(*pmeta.index_rep, pmeta.domain->second, false);
	else if (bin_range.first == 0 && bin_range.second == partio->get_num_bins())
		return RegionEncoding::make_uniform_region(*pmeta.index_rep, pmeta.domain->second, true);

	// Invoke the index decoder to determine the best region math to solve this query
	RegionMath::RegionMath rmath = this->compute_optimal_region_math_for_bin_range(*partio, bin_range, terminfo);

	// Read the regions specified
	const std::set< region_id_t > regions_to_read = rmath.get_all_regions();
	std::map< region_id_t, boost::shared_ptr< RegionEncoding > > regions;

	partio->reset_io_stats();
	assert(partio->read_regions(regions_to_read, regions));
	terminfo.binread = partio->get_io_stats();

	// Evaluate the specified region math on the regions just read
	return this->evaluate_constraint_region_math(regions, rmath, terminfo);
	TIME_STATS_TIME_END()
}

// Visitors for traversing a RegionMath, computing its result using virtual
// helper functions implemented in SimpleQueryEngine subclasses

// Evaluates the RegionMath for a given constraint, storing statistics about the process
boost::shared_ptr< RegionEncoding > SimpleQueryEngine::evaluate_constraint_region_math(
		RegionMap &rmap,
		const RegionMath::RegionMath &rmath,
		ConstraintTermEvalStats &terminfo) const
{
	class EvaluateConstraintVisitor : public EvaluateVisitor {
	public:
		typedef void result_type;
		EvaluateConstraintVisitor(const SimpleQueryEngine &qe, RegionMap &rmap) :
			EvaluateVisitor(qe), rmap(rmap) {}

		void operator()(const RegionMath::RegionTerm &op) { this->EvaluateVisitor::operator()(op); }
		void operator()(const RegionMath::UnaryOperatorTerm &op) { this->EvaluateVisitor::operator()(op); }
		void operator()(const RegionMath::NAryOperatorTerm &op) { this->EvaluateVisitor::operator()(op); }
		virtual boost::shared_ptr< RegionEncoding > obtain_region(region_id_t region) { return rmap[region]; }

	private:
		RegionMap &rmap;
	};

	TIME_STATS_TIME_BEGIN(terminfo.binmerge)
	EvaluateConstraintVisitor visitor(*this, rmap);
	for (const RegionMath::TermVariant &term : rmath)
		boost::apply_visitor(visitor, term);

	assert(visitor.stack.size() == 1);
	return visitor.stack.back();
	TIME_STATS_TIME_END()
}

// Helper functions for subclasses (in addition to this class)

// Computes the range of bins touched by a given univariate constraint
auto SimpleQueryEngine::compute_bin_range(IndexPartitionIO& partio, const ConstraintTerm& constraint) const -> bin_id_range_t {
	bin_id_range_t bin_range;

	boost::shared_ptr< const AbstractBinningSpecification > binning_spec = partio.get_partition_metadata().binning_spec;

	bin_range.first = binning_spec->upper_bound_bin(constraint.lower_bound);
	if (bin_range.first > 0) --bin_range.first; // We received the first bin after the given value. We need to rewind one bin, which will either be an equal bin or the first lesser bin

	bin_range.second = binning_spec->upper_bound_bin(constraint.upper_bound, bin_range.first); // We receive the first bin that is greater than the given value, which is the first bin we don't need, so use that bin as the upper bound

	return bin_range;
}

// Default implementation of cost computation: sum of region sizes plus a fixed cost per seek (calibrated with each other using arbitrary multiplier constants)
uint64_t SimpleQueryEngine::compute_constraint_evaluation_cost(IndexPartitionIO &partio, const RegionMath::RegionMath &rmath) const {
	// Static, arbitrary cost calibration
	static constexpr uint64_t READ_BYTE_COST = 1;
	static constexpr uint64_t SEEK_COST = 1ULL<<20;

	uint64_t cost = 0;

	const std::set< region_id_t > regions = rmath.get_all_regions();
	std::map< region_id_t, uint64_t > region_sizes; // Maps regions to their sizes in bytes

	for (region_id_t region : regions)
		region_sizes[region] = partio.compute_regions_size(region, region + 1);

	auto it = regions.cbegin();
	auto end_it = regions.cend();
	while (it != end_it) {
		// Start a run of region IDs
		const region_id_t region_run_start = *it++;
		region_id_t region_run_end = region_run_start + 1;

		// Extend the run until it ends, computing its size as we go
		uint64_t region_run_size_in_bytes = region_sizes[region_run_start];
		while (it != end_it && *it == region_run_end) {
			region_run_size_in_bytes += region_sizes[region_run_end];
			++it;
			++region_run_end;
		}

		// Add the cost
		cost += SEEK_COST + region_run_size_in_bytes * READ_BYTE_COST;
	}

	// TODO: incorporate setops cost?
	return cost;
}

// Decides between the complement and no-complement approaches to evaluating a constraint
RegionMath::RegionMath SimpleQueryEngine::compute_optimal_region_math_for_bin_range(IndexPartitionIO &partio, bin_id_range_t bin_range, ConstraintTermEvalStats &terminfo) const {
	using RMath = RegionMath::RegionMath;
	using regid_t = BinnedIndexTypes::region_id_t;
	assert(bin_range.first > 0 || bin_range.second < partio.get_num_bins());

	const IndexPartitionIO::PartitionMetadata pmeta = partio.get_partition_metadata();

	// Get an IndexEncoding object to direct the decoding process
	IndexEncoding::Type enc_type = pmeta.index_enc->get_type();
	boost::shared_ptr< IndexEncoding > enc = IndexEncoding::get_instance(enc_type);

	// Define a lambda function to help us compute the RegionMath,
	// and its associated cost, with and without complement preferred
	struct MeasuredRegionMath {
		RegionMath::RegionMath rmath;
		uint64_t cost, regioncount;
	};
	auto compute_region_math = [&](bool prefer_compl) -> MeasuredRegionMath {
		MeasuredRegionMath mrmath;
		mrmath.rmath = enc->get_region_math(pmeta.binning_spec->get_num_bins(), bin_range.first, bin_range.second, prefer_compl);
		mrmath.regioncount = mrmath.rmath.get_all_regions().size();
		mrmath.cost = this->compute_constraint_evaluation_cost(partio, mrmath.rmath);
		return mrmath;
	};

	// Compute the RegionMath/cost for complement and no-complement
	MeasuredRegionMath normal = compute_region_math(false);
	MeasuredRegionMath complement = compute_region_math(true);

	// Decide whether to use complement based on cost comparison
	using CMode = QueryEngineOptions::ComplementMode;
	const bool use_complement =
		this->options.complement_mode == CMode::ALWAYS ? true :
		this->options.complement_mode == CMode::NEVER  ? false :
		(normal.cost > complement.cost);

	// Fill up the pertinent terminfo
	terminfo.forced_complement_binmerge_mode = (this->options.complement_mode != CMode::AUTO);
	terminfo.used_complement_binmerge = use_complement;
	if (use_complement) {
		terminfo.used_binbytes = complement.cost;
		terminfo.used_bincount = complement.regioncount;
		terminfo.other_binbytes = normal.cost;
		terminfo.other_binbytes = normal.regioncount;
	} else {
		terminfo.used_binbytes = normal.cost;
		terminfo.used_bincount = normal.regioncount;
		terminfo.other_binbytes = complement.cost;
		terminfo.other_bincount = complement.regioncount;
	}

	// Return the RegionMath
	return (use_complement) ? complement.rmath : normal.rmath;
}
