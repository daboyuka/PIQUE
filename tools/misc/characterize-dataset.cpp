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
 * characterize-dataset.cpp
 *
 *  Created on: Oct 23, 2014
 *      Author: David A. Boyuka II
 */


extern "C" {
#include <getopt.h>
#ifndef _Bool
#define _Bool bool
#endif
#include <pique/util/myopts.h>
}

#include <typeindex>
#include <iostream>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <map>

#include <boost/smart_ptr.hpp>
#include <boost/optional.hpp>

#include <pique/io/database.hpp>
#include <pique/data/dataset.hpp>

#include <pique/indexing/binning-spec.hpp>

using namespace std;

struct cmd_args_t {
	char *in_metafilename_str{nullptr};

	char *binning_type_str{nullptr};
	char *binning_param_str{nullptr};

	uint64_t element_offset{0};
	uint64_t nelements{0};
};

struct cmd_config_t {
	string in_metafilename;

	AbstractBinningSpecification::BinningSpecificationType binning_type;
	std::string binning_param;

	uint64_t element_offset;
	uint64_t nelements;
};

template< typename BinningSpecificationT >
struct BinningSpecBuilder { static boost::shared_ptr< BinningSpecificationT > build(const cmd_config_t &conf) { abort(); } };

template< typename ValueTypeT >
struct BinningSpecBuilder< SigbitsBinningSpecification< ValueTypeT > > {
	static boost::shared_ptr< SigbitsBinningSpecification< ValueTypeT > > build(const cmd_config_t &conf) {
		int sigbits = strtol(conf.binning_param.c_str(), NULL, 0);
		assert(sigbits);

		std::cerr << "Using SIGBITS binning with " << sigbits << " significant bits" << std::endl;
		return boost::make_shared< SigbitsBinningSpecification< ValueTypeT > >(sigbits);
	}
};

template< typename ValueTypeT >
struct BinningSpecBuilder< PrecisionBinningSpecification< ValueTypeT > > {
	static boost::shared_ptr< PrecisionBinningSpecification< ValueTypeT > > build(const cmd_config_t &conf) {
		const int digits = strtol(conf.binning_param.c_str(), NULL, 0);
		assert(digits);

		std::cerr << "Using PRECISION binning with " << digits << " digits" << std::endl;
		return boost::make_shared< PrecisionBinningSpecification< ValueTypeT > >(digits);
	}
};

template< typename ValueTypeT >
struct BinningSpecBuilder< ExplicitBinsBinningSpecification< ValueTypeT > > {
	static boost::shared_ptr< ExplicitBinsBinningSpecification< ValueTypeT > > build(const cmd_config_t &conf) {
		using QKeyType = typename ExplicitBinsBinningSpecification< ValueTypeT >::QKeyType;

		std::fstream fin(conf.binning_param);
		assert(fin.good());

		QKeyType bin_bound;
		std::vector< QKeyType > bin_bounds;
		while (!fin.eof()) {
			assert(!fin.bad());
			fin >> bin_bound;
			bin_bounds.push_back(bin_bound);
		}

		fin.close();

		std::cerr << "Using EXPLICIT binning with " << bin_bounds.size() << " bin boundaries read from file " << conf.binning_param << std::endl;
		return boost::make_shared< ExplicitBinsBinningSpecification< ValueTypeT > >(bin_bounds);
	}
};

struct qkey_characteristics_t {
	uint64_t count{0};
	uint64_t runs{0};
};

template<typename datatype_t>
boost::shared_ptr< BufferedDatasetStream< datatype_t > > open_buffered_dataset_subset(boost::shared_ptr< Dataset > dataset, uint64_t offset, uint64_t nelem) {
	if (offset >= dataset->get_element_count())
		return nullptr;
	if (offset + nelem >= dataset->get_element_count())
		nelem = dataset->get_element_count() - offset;

	std::cerr << "Opening elements " << offset << " through " << (offset + nelem) << "..." << std::endl;

	GridSubset subset(dataset->get_grid(), offset, nelem);
	boost::shared_ptr< DatasetStream< datatype_t > > typed_dstream = boost::static_pointer_cast< DatasetStream< datatype_t > >(dataset->open_stream(subset));
	boost::shared_ptr< BufferedDatasetStream< datatype_t > > buf_dstream = boost::dynamic_pointer_cast< BufferedDatasetStream< datatype_t > >(typed_dstream);
	if (!buf_dstream)
		buf_dstream = boost::make_shared< BlockBufferingDatasetStream< datatype_t > >(typed_dstream);

	return buf_dstream;
}

template<typename datatype_t, typename BinningSpecT, typename boost::disable_if_c< BinningSpecT::is_valid_instantiation, int >::type ignore = 0>
static void do_characterize_dataset(boost::shared_ptr< Dataset > dstream, const cmd_config_t &conf)
{
	abort();
}

template<typename datatype_t, typename BinningSpecT, typename boost::enable_if_c< BinningSpecT::is_valid_instantiation, int >::type ignore = 0>
static void do_characterize_dataset(boost::shared_ptr< Dataset > dataset, const cmd_config_t &conf)
{
	static constexpr uint64_t BUFFER_SIZE = 1ULL<<24;

    using TypedBufferedDatasetStream = BufferedDatasetStream< datatype_t >;
    using data_buffer_it_t = typename TypedBufferedDatasetStream::buffer_iterator_t;
    using QKeyType = typename BinningSpecT::QKeyType;

	boost::shared_ptr< BinningSpecT > binning = BinningSpecBuilder< BinningSpecT >::build(conf);

	// Get a buffered dataset stream (use the current one if it's already buffered, otherwise wrap it in a buffer); this is to amortize virtual function call cost

	bool first = true;
	QKeyType prev, cur;
	std::map< QKeyType, qkey_characteristics_t > chars;

	const uint64_t nelements_raw = conf.nelements ? conf.nelements : dataset->get_element_count();
	const uint64_t end_offset = std::min(conf.element_offset + nelements_raw, dataset->get_element_count());
	const uint64_t nelements = end_offset - conf.element_offset;
	uint64_t offset = conf.element_offset;
	uint64_t total_count = 0;

	while (offset < end_offset) {
		boost::shared_ptr< TypedBufferedDatasetStream > buf_dstream = open_buffered_dataset_subset< datatype_t >(dataset, offset, std::min(BUFFER_SIZE, end_offset - offset));

		data_buffer_it_t in_it, in_end_it;
		while (buf_dstream->get_buffered_data(in_it, in_end_it)) {
			for (; in_it != in_end_it; ++in_it) {
				cur = binning->get_quantization().quantize(*in_it);

				if (prev != cur || first) {
					++chars[cur].runs;
				}
				++chars[cur].count;
				++total_count;
				++offset;

				first = false;
				prev = cur;
			}

			std::cerr << "Progress: " << total_count << "/" << nelements << " elements processed" << std::endl;
		}
	}

	std::cout << "Bin key\tDensity\tMean Run Length\tSigma" << std::endl;
	for (std::pair< QKeyType, qkey_characteristics_t > entry : chars) {
		const double density = (double)entry.second.count / total_count;
		const double mean_set_run_length = (double)entry.second.count / entry.second.runs;
		const double sigma = mean_set_run_length * (1 - density); // E[markov] / E[bernoulli] where E[bernoulli] = 1/(1-delta)

		std::cout << entry.first << "\t" << density << "\t" << mean_set_run_length << "\t" << sigma << std::endl;
	}
	std::cerr.flush();
}

template<typename datatype_t>
struct DispatchBinning {
	template<typename BinningSpecT>
	bool operator()(boost::shared_ptr< Dataset > dataset, const cmd_config_t &conf) const {
		do_characterize_dataset< datatype_t, BinningSpecT >(dataset, conf);
		return true;
	}
};

struct DispatchCharacterize {
	using BinningType = AbstractBinningSpecification::BinningSpecificationType;

	template<typename datatype_t>
	bool operator()(boost::shared_ptr< Dataset > dataset, const cmd_config_t &conf) const {
		typedef typename BinningSpecificationDispatch< datatype_t >::type BinningTypeDispatch;

		return BinningTypeDispatch::template dispatchMatching< bool >(conf.binning_type, DispatchBinning< datatype_t >(), false, dataset, conf);
	}
};

static void characterize_dataset(const cmd_config_t &conf) {
	Database db;
	const int varid = db.add_variable("thevar", boost::make_optional(conf.in_metafilename), boost::none);

	boost::shared_ptr< DataVariable > var = db.get_variable(varid);
	boost::shared_ptr< Dataset > dataset = var->open_dataset();

	const Datatypes::IndexableDatatypeID dtid = dataset->get_datatype();

	const bool success = Datatypes::DatatypeIDToCTypeDispatch::dispatchMatching< bool >(dtid, DispatchCharacterize(), false, dataset, conf);
	if (!success) abort();
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

static void validate_and_fully_parse_args(cmd_config_t &conf, cmd_args_t &args) {
	using BinningType = AbstractBinningSpecification::BinningSpecificationType;

	conf.in_metafilename = string(args.in_metafilename_str);
	conf.element_offset = args.element_offset;
	conf.nelements = args.nelements;

	const std::string binningtype = args.binning_type_str;
	if (binningtype == "sigbits")
		conf.binning_type = BinningType::SIGBITS;
	else if (binningtype == "precision")
		conf.binning_type = BinningType::PRECISION;
	else if (binningtype == "explicit")
		conf.binning_type = BinningType::EXPLICIT_BINS;
	else
		abort();

	conf.binning_param = args.binning_param_str;
}

int main(int argc, char **argv) {
    cmd_args_t args;
    cmd_config_t conf;

    args.element_offset = 0;
    args.nelements = 0;

    myoption opts[] = {
        addopt("metafile", required_argument, OPTION_TYPE_STRING, &args.in_metafilename_str),
		addopt("binningtype", required_argument, OPTION_TYPE_STRING, &args.binning_type_str),
		addopt("binningparam", required_argument, OPTION_TYPE_STRING, &args.binning_param_str),

        addopt("elemoff", required_argument, OPTION_TYPE_UINT64, &args.element_offset),
        addopt("nelems", required_argument, OPTION_TYPE_UINT64, &args.nelements),

        addopt(NULL, required_argument, OPTION_TYPE_BOOLEAN, NULL),
    };
    parse_args(&argc, &argv, opts);
    validate_and_fully_parse_args(conf, args);

    characterize_dataset(conf);
}


