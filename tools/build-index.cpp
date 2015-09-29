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
#include <pique/io/posix/posix-index-io.hpp>

struct cmd_args_t {
	char *dataset_meta_filename_str{nullptr};
	char *index_filename_str{nullptr};

	char *index_rep_str{nullptr};
	char *index_enc_str{nullptr};
	char *binning_type_str{nullptr};
	char *binning_param_str{nullptr};
	bool cblq_dense_suff{false};
};

struct cmd_config_t {
	std::string dataset_meta_filename;
	std::string index_filename;

	RegionEncoding::Type index_rep;
	boost::shared_ptr< IndexEncoding > index_enc;
	AbstractBinningSpecification::BinningSpecificationType binning_type;
	std::string binning_param;
	bool cblq_dense_suff;
};

template< typename BinningSpecificationT >
struct BinningSpecBuilder { static boost::shared_ptr< BinningSpecificationT > build(const cmd_config_t &conf); };

template< typename ValueTypeT >
struct BinningSpecBuilder< SigbitsBinningSpecification< ValueTypeT > > {
	static boost::shared_ptr< SigbitsBinningSpecification< ValueTypeT > > build(const cmd_config_t &conf) {
		const int sigbits = strtol(conf.binning_param.c_str(), NULL, 0);
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

		std::cerr << "Using EXPLICIT binning with " << bin_bounds.size() << " bin boundaries read from file " << conf.binning_param << std::endl;
		return boost::make_shared< ExplicitBinsBinningSpecification< ValueTypeT > >(bin_bounds);
	}
};

template<typename datatype_t, typename BinningSpecificationT, typename boost::disable_if_c< BinningSpecificationT::is_valid_instantiation, int >::type ignore = 0 >
static boost::shared_ptr< BinnedIndex > build_index(const cmd_config_t &conf, boost::shared_ptr< Dataset > dataset) {
	abort();
}

template<typename datatype_t, typename BinningSpecificationT, typename boost::enable_if_c< BinningSpecificationT::is_valid_instantiation, int >::type ignore = 0 >
static boost::shared_ptr< BinnedIndex > build_index(const cmd_config_t &conf, boost::shared_ptr< Dataset > dataset) {
	boost::shared_ptr< BinningSpecificationT > binning_spec = BinningSpecBuilder< BinningSpecificationT >::build(conf);

	switch (conf.index_rep) {
	case RegionEncoding::Type::II:
		return IndexBuilder< datatype_t, IIRegionEncoder, BinningSpecificationT >(IIRegionEncoderConfig(), binning_spec).build_index(*dataset);
	case RegionEncoding::Type::CII:
		return IndexBuilder< datatype_t, CIIRegionEncoder, BinningSpecificationT >(CIIRegionEncoderConfig(), binning_spec).build_index(*dataset);
	case RegionEncoding::Type::WAH:
		return IndexBuilder< datatype_t, WAHRegionEncoder, BinningSpecificationT >(WAHRegionEncoderConfig(), binning_spec).build_index(*dataset);
	case RegionEncoding::Type::CBLQ_2D:
		return IndexBuilder< datatype_t, CBLQRegionEncoder<2>, BinningSpecificationT >(CBLQRegionEncoderConfig(conf.cblq_dense_suff), binning_spec).build_index(*dataset);
	case RegionEncoding::Type::CBLQ_3D:
		return IndexBuilder< datatype_t, CBLQRegionEncoder<3>, BinningSpecificationT >(CBLQRegionEncoderConfig(conf.cblq_dense_suff), binning_spec).build_index(*dataset);
	case RegionEncoding::Type::CBLQ_4D:
		return IndexBuilder< datatype_t, CBLQRegionEncoder<4>, BinningSpecificationT >(CBLQRegionEncoderConfig(conf.cblq_dense_suff), binning_spec).build_index(*dataset);
	default:
		std::cerr << "Unsupported index representation " << (int)conf.index_rep << std::endl;
		abort();
	}
	return nullptr;
}

template<typename datatype_t>
boost::shared_ptr< BinnedIndex > build_index(const cmd_config_t &conf, boost::shared_ptr< Dataset > dataset) {
	using BinningSpecificationType = AbstractBinningSpecification::BinningSpecificationType;

	switch (conf.binning_type) {
	case BinningSpecificationType::SIGBITS:
		return build_index<datatype_t, SigbitsBinningSpecification< datatype_t > >(conf, dataset);
	case BinningSpecificationType::EXPLICIT_BINS:
		return build_index<datatype_t, ExplicitBinsBinningSpecification< datatype_t > >(conf, dataset);
	case BinningSpecificationType::PRECISION:
		return build_index<datatype_t, PrecisionBinningSpecification< datatype_t > >(conf, dataset);
	default:
		std::cerr << "Unsupported binning method " << (int)conf.binning_type << std::endl;
		abort();
	}
	return nullptr;
}

struct BuildIndexDatatypeHelper {
	template<typename datatype_t>
	boost::shared_ptr< BinnedIndex > operator()(const cmd_config_t &conf, boost::shared_ptr< Dataset > dataset) {
		return build_index<datatype_t>(conf, dataset);
	}
};

static boost::shared_ptr< BinnedIndex > build_index(const cmd_config_t &conf, boost::shared_ptr< Dataset > dataset) {
	boost::shared_ptr< BinnedIndex > index =
			Datatypes::DatatypeIDToCTypeDispatch::template dispatchMatching< boost::shared_ptr< BinnedIndex > >(
						dataset->get_datatype(),
						BuildIndexDatatypeHelper(),
						nullptr,
						conf, dataset
			);

	if (!index) {
		std::cerr << "Unsupported dataset datatype " << (int)dataset->get_datatype() << std::endl;
		abort();
	}
	return index;
}

static boost::shared_ptr< BinnedIndex > encode_index(const cmd_config_t &conf, boost::shared_ptr< BinnedIndex > flat_index) {
	PreferenceListSetOperations setops;
	setops.push_back(boost::make_shared< IISetOperations >(IISetOperationsConfig()));
	setops.push_back(boost::make_shared< CIISetOperations >(CIISetOperationsConfig(true)));
	setops.push_back(boost::make_shared< CBLQSetOperationsFast<2> >(CBLQSetOperationsConfig(true)));
	setops.push_back(boost::make_shared< CBLQSetOperationsFast<3> >(CBLQSetOperationsConfig(true)));
	setops.push_back(boost::make_shared< CBLQSetOperationsFast<4> >(CBLQSetOperationsConfig(true)));
	setops.push_back(boost::make_shared< WAHSetOperations >(WAHSetOperationsConfig(true)));

	return IndexEncoding::get_encoded_index(conf.index_enc, flat_index, setops);
}

static boost::shared_ptr< IndexIO > open_index_io(const cmd_config_t &conf) {
	return boost::make_shared< POSIXIndexIO >();
}

static void produce_index_file(const cmd_config_t &conf) {
	using EncType = IndexEncoding::Type;

	std::cout << "[1] Loading dataset based on metafile \"" << conf.dataset_meta_filename << "\"" << std::endl;
	DataVariable dv("thevar", conf.dataset_meta_filename, boost::none);
	boost::shared_ptr< Dataset > dataset = dv.open_dataset();
	assert(dataset /* Need successful dataset load */);

	std::cout << "[2] Building index..." << std::endl;
	boost::shared_ptr< BinnedIndex > flat_index = build_index(conf, dataset);

	std::cout << "    Index statistics:" << std::endl;
	flat_index->dump_summary();

	boost::shared_ptr< BinnedIndex > final_index;
	if (conf.index_enc->get_type() == EncType::EQUALITY) {
		final_index = flat_index;
	} else {
		std::cout << "[2.5] Encoding index..." << std::endl;
		final_index = encode_index(conf, flat_index);

		std::cout << "    Encoded index statistics:" << std::endl;
		final_index->dump_summary();
	}

	std::cout << "[3] Opening index file \"" << conf.index_filename << "\" for writing..." << std::endl;
	boost::shared_ptr< IndexIO > indexio = open_index_io(conf);
	indexio->open(conf.index_filename, IndexOpenMode::WRITE);

	std::cout << "[4] Writing index to disk..." << std::endl;
	boost::shared_ptr< IndexPartitionIO > partio = indexio->append_partition();
	partio->write_index(*final_index);

	std::cout << "[5] Finalizing index..." << std::endl;
	partio->close();
	indexio->close();
	std::cout << "[6] Done!" << std::endl;
}

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
    cmd_args_t args;
    cmd_config_t conf;

    bool TRUEVAL = true; //, FALSEVAL = false;

    myoption opts[] = {
   		addopt("metafile", required_argument, OPTION_TYPE_STRING, &args.dataset_meta_filename_str),
   		addopt("indexfile", required_argument, OPTION_TYPE_STRING, &args.index_filename_str),
		addopt("indexrep", required_argument, OPTION_TYPE_STRING, &args.index_rep_str),
		addopt("indexenc", required_argument, OPTION_TYPE_STRING, &args.index_enc_str),
		addopt("binningtype", required_argument, OPTION_TYPE_STRING, &args.binning_type_str),
		addopt("binningparam", required_argument, OPTION_TYPE_STRING, &args.binning_param_str),
		addopt("cblq_dense_suff", optional_argument, OPTION_TYPE_BOOLEAN, &args.cblq_dense_suff, &TRUEVAL),
        addopt(NULL, required_argument, OPTION_TYPE_BOOLEAN, NULL),
    };
    parse_args(&argc, &argv, opts);
    validate_and_fully_parse_args(conf, args);

    produce_index_file(conf);
}
