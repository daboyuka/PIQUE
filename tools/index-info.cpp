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
 * index-info.cpp
 *
 *  Created on: Jan 20, 2015
 *      Author: David A. Boyuka II
 */

extern "C" {
#include <getopt.h>
#ifndef _Bool
#define _Bool bool
#endif
#include <pique/util/myopts.h>
#include <unistd.h>
}

#include <cstdint>
#include <cstring>
#include <cassert>

#include <pique/util/datatypes.hpp>
#include <pique/io/posix/posix-index-io.hpp>

using namespace std;

struct cmd_args_t {
	char *index_filename;
};

struct cmd_config_t {
	std::string index_filename;
};

static bool validate_and_fully_parse_args(cmd_config_t &conf, cmd_args_t &args) {
	conf.index_filename = args.index_filename;

	if (access(conf.index_filename.c_str(), F_OK) != 0) {
		std::cerr << "Error: cannot access index file '" << conf.index_filename << "'" << std::endl;
		return false;
	}

	return true;
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

static std::string get_index_encoding_name(IndexEncoding::Type type) {
	using Type = IndexEncoding::Type;
	switch (type) {
	case Type::EQUALITY:			return "equality";
	case Type::RANGE:				return "range";
	case Type::INTERVAL:			return "interval";
	case Type::BINARY_COMPONENT:	return "binarycomp";
	case Type::HIERARCHICAL:		return "hierarchical";
	default: return "unknown";
	}
}

struct SigbitsGetName {
	template<typename ValueType>
	std::string operator()(const AbstractBinningSpecification &binning) const {
		const auto &sigbits_binning = dynamic_cast< const SigbitsBinningSpecification< ValueType > & >(binning);

		std::stringstream ss;
		ss << "sigbits(" << sigbits_binning.get_sigbits() << ")";
		return ss.str();
	}
};

struct PrecisionGetName {
	template<typename ValueType>
	std::string operator()(const AbstractBinningSpecification &binning) const {
		const auto &prec_binning = dynamic_cast< const PrecisionBinningSpecification< ValueType > & >(binning);

		std::stringstream ss;
		ss << "precision(" << prec_binning.get_digits() << ")";
		return ss.str();
	}
};

static std::string get_binning_name(const AbstractBinningSpecification &binning) {
	using Type = AbstractBinningSpecification::BinningSpecificationType;
	const Datatypes::IndexableDatatypeID dt = binning.get_binning_datatype();

	std::stringstream ss;
	switch (binning.get_binning_spec_type()) {
	case Type::EXPLICIT_BINS:
		return "exact-bins";
	case Type::PRECISION:
		return Datatypes::DatatypeIDToCTypeDispatch::template dispatchMatching< std::string >(dt, PrecisionGetName(), std::string(), binning);
	case Type::SIGBITS:
		return Datatypes::DatatypeIDToCTypeDispatch::template dispatchMatching< std::string >(dt, SigbitsGetName(), std::string(), binning);
	default: return "unknown";
	}
}

static int print_index_info(cmd_config_t &conf) {
	POSIXIndexIO iio;
	iio.open(conf.index_filename, IndexOpenMode::READ);

	IndexIO::GlobalMetadata gmeta = iio.get_global_metadata();
	std::vector< IndexIOTypes::domain_mapping_t > domain_mappings = iio.get_sorted_partition_domain_mappings();

	std::cout << "Number of partitions: " << gmeta.partitions.size() << std::endl;

	for (size_t did = 0 ; did < domain_mappings.size(); ++did) {
		const IndexIOTypes::domain_mapping_t &mapping = domain_mappings[did];
		std::cout << "Domain " << did << ": "
				  << "partitionID = " << mapping.first << ", "
				  << "bounds = [" << mapping.second.first << ", " << mapping.second.second << ")"
				  << std::endl;
	}

	for (size_t did = 0 ; did < domain_mappings.size(); ++did) {
		const IndexIOTypes::domain_mapping_t &mapping = domain_mappings[did];
		boost::shared_ptr< IndexPartitionIO > partio = iio.get_partition(mapping.first);
		IndexPartitionIO::PartitionMetadata pmeta = partio->get_partition_metadata();

		std::cout << std::endl;
		std::cout << "Domain " << did << "/Partition " << mapping.first << ": " << std::endl;
		std::cout << "  datatype: " << *Datatypes::get_name_by_datatypeid(*pmeta.indexed_datatype) << std::endl;
		std::cout << "  region rep: " << *RegionEncoding::get_region_representation_type_name(*pmeta.index_rep) << std::endl;
		std::cout << "  index encoding: " << get_index_encoding_name(pmeta.index_enc->get_type()) << std::endl;

		std::cout << "  binning: " << get_binning_name(*pmeta.binning_spec) << ", nbins = " << partio->get_num_bins() << std::endl;
		std::vector< UniversalValue > bins = pmeta.binning_spec->get_all_bin_keys();
		for (BinnedIndexTypes::bin_id_t i = 0; i < bins.size(); ++i) {
			std::cout << "    bin key " << i << ": " << bins[i].to_string() << std::endl;
		}

		std::cout << "  regions: " << partio->get_num_regions() << std::endl;
		for (BinnedIndexTypes::region_id_t i = 0; i < partio->get_num_regions(); ++i) {
			std::cout << "    region " << i << " bytes: " << partio->compute_regions_size(i, i + 1) << std::endl;
		}
	}

	return 0;
}

int main(int argc, char **argv) {
    cmd_args_t args;
    cmd_config_t conf;

    //bool TRUEVAL = true;

    myoption opts[] = {
        addopt("indexfile", required_argument, OPTION_TYPE_STRING, &args.index_filename),
        addopt(NULL, required_argument, OPTION_TYPE_BOOLEAN, NULL),
    };
    parse_args(&argc, &argv, opts);
    if (!validate_and_fully_parse_args(conf, args)) return -1;

    return print_index_info(conf);
}
