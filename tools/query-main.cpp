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
 * build-meta.cpp
 *
 *  Created on: May 20, 2014
 *      Author: David A. Boyuka II
 */

extern "C" {
#include <getopt.h>
#ifndef _Bool
#define _Bool bool
#endif
#include <pique/util/myopts.h>
}

#include <cctype>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <iostream>
#include <string>

#include <boost/smart_ptr.hpp>

#include <pique/query/query.hpp>

#include "query-helper.hpp"

struct cmd_args_t {
	const char *dbfile {nullptr};
	const char *ridfile {nullptr};
	const char *qemode {"basic"};
	const char *setopsmode {"fastnary3"};
	const char *convmode {"df"};
	const char *complmode {"auto"};
};

struct cmd_config_t {
	std::string dbfile;
	boost::optional< std::string > ridfile;
	std::string qemode, setopsmode, convmode;
	QueryEngineOptions qeoptions;
};

static void validate_and_fully_parse_args(cmd_config_t &conf, cmd_args_t &args) {
	using ComplMode = QueryEngineOptions::ComplementMode;

	assert(args.dbfile);
	conf.dbfile = args.dbfile;
	conf.ridfile = args.ridfile ? boost::make_optional(std::string(args.ridfile)) : boost::none;

	conf.qemode		= args.qemode;
	conf.setopsmode	= args.setopsmode;
	conf.convmode	= args.convmode;

	conf.qeoptions.complement_mode =
		(strcasecmp(args.complmode, "auto") == 0) ? ComplMode::AUTO :
		(strcasecmp(args.complmode, "never") == 0) ? ComplMode::NEVER :
		(strcasecmp(args.complmode, "always") == 0) ? ComplMode::ALWAYS :
		(abort(), ComplMode::AUTO);
}

static myoption addopt(const char *flagname, int hasarg, OPTION_VALUE_TYPE type, void *output, void *fixedval = NULL) {
	myoption opt;
	opt.flagname = flagname;
	opt.hasarg = hasarg;
	opt.type = type;
	opt.output = output;
	opt.fixedval = fixedval;
	return opt;
}

static Query parse_query(int argc, char **argv) {
	enum struct Relation { NONE, LT, GT, LE, GE, /*EQ,*/ };

	Query query;

	while (argc--) {
		char *term = *argv++;

		if (strcasecmp(term, "AND") == 0) {
			query.push_back(boost::make_shared< NAryOperatorTerm >(NArySetOperation::INTERSECTION));
			continue;
		} else if (strcasecmp(term, "OR") == 0) {
			query.push_back(boost::make_shared< NAryOperatorTerm >(NArySetOperation::UNION));
			continue;
		} else if (strcasecmp(term, "NOT") == 0) {
			query.push_back(boost::make_shared< UnaryOperatorTerm >(UnarySetOperation::COMPLEMENT));
			continue;
		}

		// Constraint term case

		Relation boundtypes[2] { Relation::NONE, Relation::NONE };
		char *parts[3] { term, nullptr, nullptr };
		int nparts = 1;

		char *boundptr;
		if ((boundptr = strchr(parts[0], '>')) != nullptr ||
			(boundptr = strchr(parts[0], '<')) != nullptr)
		{
			if (boundptr[0] == '<') {
				if (boundptr[1] == '=') { boundtypes[0] = Relation::LE; }
				else                    { boundtypes[0] = Relation::LT; }
			} else { // boundptr[0] == '>'
				if (boundptr[1] == '=') { boundtypes[0] = Relation::GE; }
				else                    { boundtypes[0] = Relation::GT; }
			}

			*boundptr++ = 0;
			if (*boundptr == '=')
				*boundptr++ = 0;

			parts[1] = boundptr;
			++nparts;
		}

		if (parts[1] && (
			  (boundptr = strchr(parts[1], '>')) != nullptr ||
			  (boundptr = strchr(parts[1], '<')) != nullptr))
		{
			if (boundptr[0] == '<') {
				if (boundptr[1] == '=') { boundtypes[1] = Relation::LE; }
				else                    { boundtypes[1] = Relation::LT; }
			} else { // boundptr[0] == '>'
				if (boundptr[1] == '=') { boundtypes[1] = Relation::GE; }
				else                    { boundtypes[1] = Relation::GT; }
			}

			*boundptr++ = 0;
			if (*boundptr == '=')
				*boundptr++ = 0;

			parts[2] = boundptr;
			++nparts;
		}

		assert(nparts >= 2 /* at least one bound must be specified */);

		for (int i = 0; i < nparts; ++i) {
			// Trim leading space
			while (isspace(*parts[i]))
				++parts[i];

			// Trim trailing space
			for (char *ptr = parts[i]; *ptr; ++ptr) {
				if (isspace(*ptr)) {
					*ptr = 0;
					break;
				}
			}
		}

		UniversalValue lb = -std::numeric_limits< double >::infinity();
		UniversalValue ub = std::numeric_limits< double >::infinity();
		std::string varname;

		if (nparts == 3) {
			UniversalValue bounds[2];
			// If we have 3 parts, then [0] and [2] are the bounds
			char *afterval;
			bounds[0] = UniversalValue(strtod(parts[0], &afterval));
			assert(parts[0] != afterval);

			bounds[1] = UniversalValue(strtod(parts[2], &afterval));
			assert(parts[2] != afterval);

			if (boundtypes[0] == Relation::LE || boundtypes[0] == Relation::LT) {
				assert(boundtypes[1] == Relation::LE || boundtypes[1] == Relation::LT);
				lb = bounds[0];
				ub = bounds[1];
			} else {
				assert(boundtypes[1] == Relation::GE || boundtypes[1] == Relation::GT);
				ub = bounds[0];
				lb = bounds[1];
			}

			varname = parts[1];
		} else {
			// Otherwise, we have 2 parts, and either could be the bound
			// Pick the one that is a number to be the bound
			char *afterval;
			bool left_part_is_bound = true;
			double partval = strtod(parts[0], &afterval);
			if (afterval == parts[0]) {
				partval = strtod(parts[1], &afterval);
				assert(parts[1] != afterval);
				left_part_is_bound = false;
			}

			if ((boundtypes[0] == Relation::LE || boundtypes[0] == Relation::LT) == left_part_is_bound) {
				lb = UniversalValue(partval);
			} else {
				ub = UniversalValue(partval);
			}

			varname = left_part_is_bound ? parts[1] : parts[0];
		}

		query.push_back(boost::make_shared< ConstraintTerm >(varname, lb, ub));
	}

	return query;
}

static int run_expr_query(const cmd_config_t &conf, int argc, char **argv) {
	Query query = parse_query(argc, argv);

	std::cout << "Query:";
	for (boost::shared_ptr< QueryTerm > qt : query) {
		std::cout << " " << qt->to_string();
	}
	std::cout << std::endl;

	boost::shared_ptr< Database > db = Database::load_from_file(conf.dbfile);
	boost::shared_ptr< QueryEngine > qe = configure_query_engine(conf.qemode, conf.setopsmode, conf.convmode, conf.qeoptions, true);

	IndexIOTypes::partition_count_t total_domains = 0;
	uint64_t total_results = 0;

	std::fstream ridsout;
	std::vector< uint64_t > rids;
	if (conf.ridfile) {
		ridsout.open(*conf.ridfile, std::ios::out | std::ios::trunc);
		assert(ridsout.good());
	}

	qe->open(db);

	boost::shared_ptr< QueryEngine::QueryCursor > cursor = qe->evaluate(query);
	while (cursor->has_next()) {
		QueryEngine::QueryPartitionResult part_result = cursor->next();

		uint64_t part_result_count;
		double rid_count_or_conv_time = 0;

		if (conf.ridfile) {
			// Convert the result to RIDs, storing in a vector for dumping to file
			TIME_BLOCK_BEGIN(&rid_count_or_conv_time, nullptr)
			part_result.result->convert_to_rids(rids, true, false);
			TIME_BLOCK_END()

			part_result_count = rids.size();

			// Write the RIDs to file, in binary form
			ridsout.write(reinterpret_cast<char*>(&rids.front()), rids.size() * sizeof(uint64_t));
			assert(ridsout.good());
		} else {
			TIME_BLOCK_BEGIN(&rid_count_or_conv_time, nullptr)
			part_result_count = part_result.result->get_element_count();
			TIME_BLOCK_END()
		}

		std::vector< QueryEngine::ConstraintTermEvalStats > constraint_term_stats;
		for (const auto &term_ptr : part_result.stats.terminfos) {
			const QueryEngine::TermEvalStats &term = *term_ptr;
			const std::type_info &ti = typeid(term);
			if (ti == typeid(QueryEngine::ConstraintTermEvalStats)) {
				const QueryEngine::ConstraintTermEvalStats &cterm = dynamic_cast< const QueryEngine::ConstraintTermEvalStats & >(term);
				constraint_term_stats.push_back(cterm);
			}
		}

		uint64_t total_bins_touched = 0;
		size_t num_constraints = 0;
		size_t num_complements_used = 0;
		for (const auto &cterm : constraint_term_stats) {
			++num_constraints;
			num_complements_used += cterm.used_complement_binmerge ? 1 : 0;
			total_bins_touched += cterm.used_bincount;
		}

		std::cerr << "Domain " << total_domains << ": "
				  << "bounds = [" << part_result.partition_domain.first << "," << part_result.partition_domain.second << "), "
				  << "partition ID = " << part_result.partition << ", "
				  << "result count = " << part_result_count
				  << std::endl;
		std::cerr << "> Stats: "
				  << "total time = " << (part_result.stats.total.time + rid_count_or_conv_time) << "s, "
				  << "IO read time = " << part_result.stats.iototal.read_time << "s, "
				  << "IO read bytes = " << part_result.stats.iototal.read_bytes << ", "
				  << "IO read seeks = " << part_result.stats.iototal.read_seeks << ", "
				  << "decode time = " << part_result.stats.decodetotal.time << ", "
				  << "setops time = " << part_result.stats.setopstotal.time << ", "
				  << "ridconv time = " << rid_count_or_conv_time << ", "
				  << "bins touched = " << total_bins_touched << ", "
				  << "bin complements used = " << num_complements_used
				  << std::endl;
		std::cerr << "> More stats: ";
		for (const auto &cterm : constraint_term_stats) {
			std::cerr << cterm.name << " bins touched = " << cterm.used_bincount << ", "
			          << cterm.name << " complement used = " << (cterm.used_complement_binmerge ? "true" : "false") << ", ";
		}
		std::cerr << std::endl;

		++total_domains;
		total_results += part_result_count;
	}

	qe->close();
	if (conf.ridfile)
		ridsout.close();

	std::cerr << "Total partitions: " << total_domains << std::endl;
	std::cerr << "Total results: " << total_results << std::endl;

	return 0;
}

static int run_query(const cmd_config_t &conf, int argc, char **argv) {
	return run_expr_query(conf, argc, argv);
}

int main(int argc, char **argv) {
    cmd_args_t args;
    cmd_config_t conf;

    //static bool TRUEVAL = true, FALSEVAL = false;

    myoption opts[] = {
      	addopt("dbfile", required_argument, OPTION_TYPE_STRING, &args.dbfile),
      	addopt("ridfile", required_argument, OPTION_TYPE_STRING, &args.ridfile),
      	addopt("qemode", required_argument, OPTION_TYPE_STRING, &args.qemode),
      	addopt("setopsmode", required_argument, OPTION_TYPE_STRING, &args.setopsmode),
      	addopt("convmode", required_argument, OPTION_TYPE_STRING, &args.convmode),
      	addopt("complmode", required_argument, OPTION_TYPE_STRING, &args.complmode),
        addopt(NULL, required_argument, OPTION_TYPE_BOOLEAN, NULL),
    };
    parse_args(&argc, &argv, opts);
    validate_and_fully_parse_args(conf, args);

    return run_query(conf, argc, argv);
}
