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

#include <cstdint>
#include <cstring>
#include <cassert>
#include <typeindex>
#include <iostream>
#include <fstream>

#include <boost/none.hpp>
#include <boost/optional.hpp>
#include <boost/algorithm/string/split.hpp>

#include <mpi.h>

#include <pique/data/dataset.hpp>
#include <pique/io/database.hpp>

#include <pique/indexing/binned-index.hpp>
#include <pique/indexing/index-builder.hpp>
#include <pique/region/region-encoding.hpp>

#include <pique/region/ii/ii.hpp>
#include <pique/region/ii/ii-encode.hpp>
#include <pique/setops/ii/ii-setops.hpp>
#include <pique/region/cii/cii.hpp>
#include <pique/region/cii/cii-encode.hpp>
#include <pique/setops/cii/cii-setops.hpp>
#include <pique/region/cblq/cblq.hpp>
#include <pique/region/cblq/cblq-encode.hpp>
#include <pique/setops/cblq/cblq-setops.hpp>
#include <pique/region/wah/wah.hpp>
#include <pique/region/wah/wah-encode.hpp>
#include <pique/setops/wah/wah-setops.hpp>

#include <pique/io/index-io.hpp>
#include <pique/parallel/util/gather-serializable.hpp>
#include <pique/parallel/io/mpi-index-io.hpp>
#include <pique/parallel/indexing/parallel-index-generator.hpp>

#include <pique/util/serializable-chunk.hpp>

struct cmd_args_t {
	char *dataset_meta_filename_str;
	char *index_filename_str;

	char *index_rep_str;
	char *index_enc_str;
	char *binning_type_str;
	char *binning_param_str;
	bool cblq_dense_suff;

	uint64_t partition_size;
	bool dedicated_master;

	bool verbose {false};
	uint64_t offset {0};
	uint64_t nelem {UINT64_MAX};
	bool relativize_subdomain {true};
};

struct cmd_config_t {
	std::string dataset_meta_filename;
	std::string index_filename;

	RegionEncoding::Type index_rep;
	boost::shared_ptr< IndexEncoding > index_enc;
	AbstractBinningSpecification::BinningSpecificationType binning_type;
	std::string binning_param;
	bool cblq_dense_suff;

	uint64_t partition_size;
	bool dedicated_master;

	bool verbose;
	uint64_t offset, nelem;
	bool relativize_subdomain;
};

// MPI rank and size stored globally
int rank, size;

/*
static std::ostream & print_rank() {
	return std::cerr << "[" << rank << "/" << size << "] ";
}

static void print_parallel_index_generator_stats(ParallelIndexingStats stats) {
	if (rank > 0) MPI_Recv(nullptr, 0, MPI_BYTE, rank-1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

	print_rank() << "Elements indexed: " << stats.num_elements_indexed << std::endl;
	print_rank() << "Partitions indexed: " << stats.num_partitions_indexed << std::endl;
	print_rank() << "Total index bins: " << stats.indexing_stats.num_bins << std::endl;
	print_rank() << "Dataset read bytes/seeks/buffer blocks/time: "
				 << stats.indexing_stats.iostats.read_bytes << "/"
				 << stats.indexing_stats.iostats.read_seeks << "/"
				 << stats.indexing_stats.num_read_buffer_blocks << "/"
				 << stats.indexing_stats.iostats.read_time << std::endl;
	print_rank() << "Indexing time: " << stats.indexing_stats.indexingtime.time << std::endl;
	print_rank() << "Indexing remainder time (total - datasetread - indexing): "
			     << (stats.indexing_stats.totaltime - stats.indexing_stats.indexingtime - stats.indexing_stats.iostats.get_read_as_time_stats()).time << std::endl;
	print_rank() << "Index write bytes/seeks/time: "
			 << stats.indexio_stats.write_bytes << "/"
			 << stats.indexio_stats.write_seeks << "/"
			 << stats.indexio_stats.write_time << std::endl;
	print_rank() << "MPI time: " << stats.mpistats.mpistats.mpitime.time << std::endl;
	print_rank() << "MPI read reqs/write reqs/seeks: "
		     	 << stats.mpistats.num_read_reqs << "/"
		     	 << stats.mpistats.num_write_reqs << "/"
			     << stats.mpistats.num_seeks << std::endl;
	std::cerr.flush();

	if (rank < size - 1) MPI_Send(nullptr, 0, MPI_BYTE, rank+1, 0, MPI_COMM_WORLD);
}
*/

static boost::shared_ptr< AbstractSetOperations > build_index_encode_setops() {
	boost::shared_ptr< PreferenceListSetOperations > setops = boost::make_shared< PreferenceListSetOperations >();
	setops->push_back(boost::make_shared< IISetOperations >(IISetOperationsConfig()));
	setops->push_back(boost::make_shared< CIISetOperations >(CIISetOperationsConfig(true)));
	setops->push_back(boost::make_shared< CBLQSetOperations<2> >(CBLQSetOperationsConfig(true)));
	setops->push_back(boost::make_shared< CBLQSetOperations<3> >(CBLQSetOperationsConfig(true)));
	setops->push_back(boost::make_shared< CBLQSetOperations<4> >(CBLQSetOperationsConfig(true)));
	setops->push_back(boost::make_shared< WAHSetOperations >(WAHSetOperationsConfig(true)));
	return setops;
}

namespace LuaStrings {
	static const std::string begin_table = "{";
	static const std::string end_table = "}";
	static const std::string begin_str_key = "[\"";
	static const std::string end_str_key = "\"] = ";
	static const std::string begin_value = "";
	static const std::string end_value = ", ";
};

static std::ostream & operator<<(std::ostream &o, TimeStats &stats) {
	return o << stats.time; // Don't bother with CPU-only time, it's notoriously inaccurate...
}

static std::ostream & operator<<(std::ostream &o, IOStats &stats) {
	using namespace LuaStrings;
	return o << begin_table <<
		begin_str_key << "read-time"   << end_str_key << begin_value << stats.read_time   << end_value <<
		begin_str_key << "read-seeks"  << end_str_key << begin_value << stats.read_seeks  << end_value <<
		begin_str_key << "read-bytes"  << end_str_key << begin_value << stats.read_bytes  << end_value <<
		begin_str_key << "write-time"  << end_str_key << begin_value << stats.write_time  << end_value <<
		begin_str_key << "write-seeks" << end_str_key << begin_value << stats.write_seeks << end_value <<
		begin_str_key << "write-bytes" << end_str_key << begin_value << stats.write_bytes << end_value << end_table;
}

static std::ostream & operator<<(std::ostream &o, MPIIndexIOStats &stats) {
	using namespace LuaStrings;
	return o << begin_table <<
		begin_str_key << "time"             << end_str_key << begin_value << stats.mpistats.mpitime << end_value <<
		begin_str_key << "num-mpiio-reads"  << end_str_key << begin_value << stats.num_read_reqs    << end_value <<
		begin_str_key << "num-mpiio-writes" << end_str_key << begin_value << stats.num_write_reqs   << end_value <<
		begin_str_key << "num-mpiio-seeks"  << end_str_key << begin_value << stats.num_seeks        << end_value << end_table;
}

static std::ostream & operator<<(std::ostream &o, IndexBuilderStats &stats) {
	using namespace LuaStrings;
	return o << begin_table <<
		begin_str_key << "nbins"         << end_str_key << begin_value << stats.num_bins     << end_value <<
		begin_str_key << "indexing-time" << end_str_key << begin_value << stats.indexingtime << end_value <<
		begin_str_key << "io-stats"      << end_str_key << begin_value << stats.iostats      << end_value <<
		begin_str_key << "total-time"    << end_str_key << begin_value << stats.totaltime    << end_value << end_table;
}

static std::ostream & operator<<(std::ostream &o, ParallelIndexingStats &stats) {
	using namespace LuaStrings;
	return o << begin_table <<
		begin_str_key << "parts-indexed"  << end_str_key << begin_value << stats.num_partitions_indexed << end_value <<
		begin_str_key << "elems-indexed"  << end_str_key << begin_value << stats.num_elements_indexed   << end_value <<
		begin_str_key << "total-time"     << end_str_key << begin_value << stats.total_time     << end_value <<
		begin_str_key << "indexing-stats" << end_str_key << begin_value << stats.indexing_stats << end_value <<
		begin_str_key << "io-stats"       << end_str_key << begin_value << stats.indexio_stats  << end_value <<
		begin_str_key << "mpi-stats"      << end_str_key << begin_value << stats.mpistats       << end_value <<
		end_table << std::endl;
}

static std::ostream & operator<<(std::ostream &o, std::vector< ParallelIndexingStats > &allstats) {
	using namespace LuaStrings;
	o << begin_table;
	for (size_t i = 0; i < allstats.size(); ++i)
		o << begin_value << allstats[i] << end_value;
	return o << end_table << std::endl;
}

template<typename datatype_t, typename RegionEncoderT, typename BinningSpecificationT >
bool produce_index_file(const cmd_config_t &conf, typename RegionEncoderT::RegionEncoderConfig encoder_conf, boost::shared_ptr< BinningSpecificationT > binning_spec, boost::shared_ptr< Dataset > dataset) {
	ParallelIndexGenerator< datatype_t, RegionEncoderT, BinningSpecificationT >
		pll_indexer(MPI_COMM_WORLD, encoder_conf, binning_spec, conf.index_enc, build_index_encode_setops());

	if (conf.verbose) std::cerr << "[" << rank << "/" << size << "] Beginning parallel indexing (dedicated master? " << (conf.dedicated_master ? "yes" : "no") << ")..." << std::endl;
	pll_indexer.generate_index(conf.index_filename, *dataset, conf.partition_size, conf.dedicated_master, conf.offset, conf.nelem, conf.relativize_subdomain);
	if (conf.verbose) std::cerr << "[" << rank << "/" << size << "] Done!" << std::endl;

	boost::optional< std::vector< ParallelIndexingStats > > opt_allstats = gather_serializables(pll_indexer.get_stats()); // Collect each rank's stats to a master rank
	if (opt_allstats) std::cout << *opt_allstats; // If we are the master (i.e., we have collected stats), print the stats

	return true;
}

// BEGIN DISPATCH CHAIN (converts runtime parameters, like index rep type and binning type, into compile-time template parameters)

template< typename BinningSpecificationT >
struct BinningSpecBuilder { static boost::shared_ptr< BinningSpecificationT > build(const cmd_config_t &conf); };

template< typename ValueTypeT >
struct BinningSpecBuilder< SigbitsBinningSpecification< ValueTypeT > > {
	static boost::shared_ptr< SigbitsBinningSpecification< ValueTypeT > > build(const cmd_config_t &conf) {
		int sigbits = strtol(conf.binning_param.c_str(), NULL, 0);
		assert(sigbits);

		if (conf.verbose) std::cerr << "Using SIGBITS binning with " << sigbits << " significant bits" << std::endl;
		return boost::make_shared< SigbitsBinningSpecification< ValueTypeT > >(sigbits);
	}
};

template< typename ValueTypeT >
struct BinningSpecBuilder< PrecisionBinningSpecification< ValueTypeT > > {
	static boost::shared_ptr< PrecisionBinningSpecification< ValueTypeT > > build(const cmd_config_t &conf) {
		const int digits = strtol(conf.binning_param.c_str(), NULL, 0);
		assert(digits);

		if (conf.verbose) std::cerr << "Using PRECISION binning with " << digits << " digits" << std::endl;
		return boost::make_shared< PrecisionBinningSpecification< ValueTypeT > >(digits);
	}
};

template< typename ValueTypeT >
struct BinningSpecBuilder< ExplicitBinsBinningSpecification< ValueTypeT > > {
	static boost::shared_ptr< ExplicitBinsBinningSpecification< ValueTypeT > > build(const cmd_config_t &conf) {
		using binning_datatype_t = typename ExplicitBinsBinningSpecification< ValueTypeT >::QKeyType;

		std::fstream fin(conf.binning_param);
		assert(fin.good());

		binning_datatype_t bin_bound;
		std::vector< binning_datatype_t > bin_bounds;
		while (!fin.eof()) {
			assert(!fin.bad());
			fin >> bin_bound;
			bin_bounds.push_back(bin_bound);
		}

		fin.close();

		if (conf.verbose) std::cerr << "Using EXPLICIT binning with " << bin_bounds.size() << " bin boundaries read from file " << conf.binning_param << std::endl;
		return boost::make_shared< ExplicitBinsBinningSpecification< ValueTypeT > >(bin_bounds);
	}
};

template<typename datatype_t, typename RegionEncoderT>
struct DispatchBinningSpec {
	using RegionEncoderConfig = typename RegionEncoderT::RegionEncoderConfig;

	template<typename BinningSpecT, typename boost::disable_if_c< BinningSpecT::is_valid_instantiation, int >::type ignore = 0>
	bool operator()(const cmd_config_t &conf, RegionEncoderConfig encoder_conf, boost::shared_ptr< Dataset > dataset) { return false; }

	template<typename BinningSpecT, typename boost::enable_if_c< BinningSpecT::is_valid_instantiation, int >::type ignore = 0>
	bool operator()(const cmd_config_t &conf, RegionEncoderConfig encoder_conf, boost::shared_ptr< Dataset > dataset) {
		boost::shared_ptr< BinningSpecT > binning_spec = BinningSpecBuilder< BinningSpecT >::build(conf);
		return produce_index_file< datatype_t, RegionEncoderT, BinningSpecT >(conf, encoder_conf, binning_spec, dataset);
	}
};

template<typename RegionEncoderConfigT> auto make_encoder_config(const cmd_config_t &conf) -> RegionEncoderConfigT { return RegionEncoderConfigT(); }
template<> auto make_encoder_config< CBLQRegionEncoderConfig >(const cmd_config_t &conf) -> CBLQRegionEncoderConfig { return CBLQRegionEncoderConfig(conf.cblq_dense_suff); }

template<typename datatype_t>
struct DispatchRegionEncoder {
	using BinningType = AbstractBinningSpecification::BinningSpecificationType;
	typedef typename BinningSpecificationDispatch< datatype_t >::type BinningTypeToBinningDispatch;

	template<typename RegionEncoderT>
	bool operator()(const cmd_config_t &conf, boost::shared_ptr< Dataset > dataset) {
		typedef typename RegionEncoderT::RegionEncoderConfig RegionEncoderConfig;
		const RegionEncoderConfig encoder_conf = make_encoder_config< RegionEncoderConfig >(conf);
		return BinningTypeToBinningDispatch::template dispatchMatching<bool>(conf.binning_type, DispatchBinningSpec< datatype_t, RegionEncoderT >(), false, conf, encoder_conf, dataset);
	}
};

struct DispatchDatatype {
	using RepType = RegionEncoding::Type;
	typedef typename
			MakeValueToTypeDispatch< RepType >::
				WithValues< RepType::II, RepType::CII, RepType::WAH, RepType::CBLQ_2D, RepType::CBLQ_3D, RepType::CBLQ_4D >::
				WithTypes< IIRegionEncoder, CIIRegionEncoder, WAHRegionEncoder, CBLQRegionEncoder<2>, CBLQRegionEncoder<3>, CBLQRegionEncoder<4> >::type
			RepTypeToEncoderDispatch;

	template<typename datatype_t>
	bool operator()(const cmd_config_t &conf, boost::shared_ptr< Dataset > dataset) {
		return RepTypeToEncoderDispatch::template dispatchMatching<bool>(conf.index_rep, DispatchRegionEncoder< datatype_t >(), false, conf, dataset);
	}
};

void produce_index_file(const cmd_config_t &conf) {
	if (conf.verbose) std::cerr << "[" << rank << "/" << size << "] Opening dataset based on metafile \"" << conf.dataset_meta_filename << "\"" << std::endl;
	DataVariable dv("thevar", conf.dataset_meta_filename, boost::none);
	boost::shared_ptr< Dataset > dataset = dv.open_dataset();
	assert(dataset /* Need successful dataset load */);

	Datatypes::DatatypeIDToCTypeDispatch::template dispatchMatching<bool>(dataset->get_datatype(), DispatchDatatype(), false, conf, dataset);
}

// END DISPATCH CHAIN

static void validate_and_fully_parse_args(cmd_config_t &conf, cmd_args_t &args) {
	using EncType = IndexEncoding::Type;
	using BinningType = AbstractBinningSpecification::BinningSpecificationType;

	conf.dataset_meta_filename = args.dataset_meta_filename_str;

	conf.index_filename = args.index_filename_str;

	const boost::optional< RegionEncoding::Type > reptype = RegionEncoding::get_region_representation_type_by_name(args.index_rep_str);
	assert(reptype /* Need valid index representation type */);
	conf.index_rep = *reptype;

	std::string enctype = args.index_enc_str;
	if (enctype == "flat")
		conf.index_enc = IndexEncoding::get_instance(EncType::EQUALITY);
	else if (enctype == "range")
		conf.index_enc = IndexEncoding::get_instance(EncType::RANGE);
	else if (enctype == "interval")
		conf.index_enc = IndexEncoding::get_instance(EncType::INTERVAL);
	else if (enctype == "hier")
		conf.index_enc = IndexEncoding::get_instance(EncType::HIERARCHICAL);
	else if (enctype == "binarycomp")
		conf.index_enc = IndexEncoding::get_instance(EncType::BINARY_COMPONENT);
	else
		abort();

	std::string binningtype = args.binning_type_str;
	if (binningtype == "sigbits")
		conf.binning_type = BinningType::SIGBITS;
	else if (binningtype == "explicit")
		conf.binning_type = BinningType::EXPLICIT_BINS;
	else if (binningtype == "precision")
		conf.binning_type = BinningType::PRECISION;
	else
		abort();

	conf.binning_param = std::string(args.binning_param_str);

	conf.cblq_dense_suff = args.cblq_dense_suff;
	conf.partition_size = args.partition_size;
	conf.dedicated_master = args.dedicated_master;
	conf.verbose = args.verbose;
	conf.offset = args.offset;
	conf.nelem = args.nelem;
	conf.relativize_subdomain = args.relativize_subdomain;
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

int main(int argc, char **argv) {
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

    cmd_args_t args;
    cmd_config_t conf;

    bool TRUEVAL = true, FALSEVAL = false;

    myoption opts[] = {
   		addopt("metafile", required_argument, OPTION_TYPE_STRING, &args.dataset_meta_filename_str),
   		addopt("indexfile", required_argument, OPTION_TYPE_STRING, &args.index_filename_str),
		addopt("indexrep", required_argument, OPTION_TYPE_STRING, &args.index_rep_str),
		addopt("indexenc", required_argument, OPTION_TYPE_STRING, &args.index_enc_str),
		addopt("binningtype", required_argument, OPTION_TYPE_STRING, &args.binning_type_str),
		addopt("binningparam", required_argument, OPTION_TYPE_STRING, &args.binning_param_str),
		addopt("cblq_dense_suff", optional_argument, OPTION_TYPE_BOOLEAN, &args.cblq_dense_suff, &TRUEVAL),
		addopt("partsize", required_argument, OPTION_TYPE_UINT64, &args.partition_size),
		addopt("dedmaster", required_argument, OPTION_TYPE_BOOLEAN, &args.dedicated_master, &FALSEVAL),
		addopt("verbose", no_argument, OPTION_TYPE_BOOLEAN, &args.verbose, &TRUEVAL),
		addopt("offset", required_argument, OPTION_TYPE_UINT64, &args.offset),
		addopt("nelem", required_argument, OPTION_TYPE_UINT64, &args.nelem),
		addopt("absrids", no_argument, OPTION_TYPE_BOOLEAN, &args.relativize_subdomain, &FALSEVAL),
        addopt(NULL, required_argument, OPTION_TYPE_BOOLEAN, NULL),
    };
    parse_args(&argc, &argv, opts);
    validate_and_fully_parse_args(conf, args);

    produce_index_file(conf);

    MPI_Finalize();
}
