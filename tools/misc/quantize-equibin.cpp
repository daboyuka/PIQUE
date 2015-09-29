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
 * quantize.cpp
 *
 *  Created on: May 22, 2014
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
#include <set>
#include <map>

#include <boost/algorithm/string.hpp>
#include <boost/optional.hpp>

using namespace std;

struct cmd_args_t {
	char *in_filename_str;
	char *out_filename_str;
	char *datatype;
	char *outdatatype;

	int equibin_nbins;
	int equibin_sampling_ratio;
};

struct cmd_config_t {
	string in_filename;
	string out_filename;
	boost::optional< std::type_index > datatype;
	boost::optional< std::type_index > outdatatype;

	int equibin_nbins;
	int equibin_sampling_ratio;
};

template<typename WordT, typename OutWordT>
static void quantize_equalbinning(const cmd_config_t &conf) {
	const int nbins = conf.equibin_nbins;
	const int sampling_ratio = conf.equibin_sampling_ratio;

	FILE *fin = std::fopen(conf.in_filename.c_str(), "r");
	FILE *fout = std::fopen(conf.out_filename.c_str(), "w");
	assert(fin && fout);

	std::set< WordT > keys;

	const uint64_t BUFFER_SIZE = 1048576;
	std::vector<WordT> buf(BUFFER_SIZE);

	while (!feof(fin)) {
		const size_t nwords = fread(&buf.front(), sizeof(WordT), buf.size(), fin);
		for (size_t i = 0; i < nwords; i++)
			if ((rand() % sampling_ratio) == 0)
				keys.insert(buf[i]);
	}

	fseek(fin, 0, SEEK_SET);

	OutWordT nextbid = 0;
	std::map< WordT, OutWordT > bin_boundary_map; // each entry is the exclusive upper bound of a bin (i.e., entry (key, binid) maps values [prev_key, key) -> binid)
	uint64_t rank = 0;
	for (auto it = keys.begin(); it != keys.end(); it++, rank++) {
		OutWordT bid = (rank + 1) * nbins / keys.size() - 1; // So that bin 0 isn't generated right away
		if (bid == nextbid) {
			if (bin_boundary_map.find(*it) == bin_boundary_map.end()) // Only add this value as a bin boundary if it's not already used as one somewhere else
				bin_boundary_map.insert(std::make_pair(*it, nextbid));
			nextbid++;
		}
	}

	// The above loop is a bit sloppy and may introduce one last bin that is very small
	// If so, delete that boundary so it merges with the previous bin
	if (bin_boundary_map.size() == nbins)
		bin_boundary_map.erase(--bin_boundary_map.end());

	for (auto it = bin_boundary_map.cbegin(); it != bin_boundary_map.cend(); ++it)
		std::cout << it->first << "\t" << it->second << std::endl;

	std::vector<OutWordT> outbuf(BUFFER_SIZE);

	while (!feof(fin)) {
		const size_t nwords = fread(&buf.front(), sizeof(WordT), buf.size(), fin);
		for (size_t i = 0; i < nwords; i++) {
			auto bin_boundary_it = bin_boundary_map.lower_bound(buf[i]); // First boundary > (not >=) this element
			OutWordT binid = bin_boundary_it != bin_boundary_map.end() ? bin_boundary_it->second : nbins - 1; // The iterator will always point to a boundary, unless we are on or after the last boundary, in which case we are in the last bin
			outbuf[i] = binid;
		}
		fwrite(&outbuf.front(), sizeof(OutWordT), nwords, fout);
	}

	fclose(fin);
	fclose(fout);
}

static const std::type_info & datatype_name_to_type(string dtname) {
	if (dtname == "double")
		return typeid(double);
	else if (dtname == "uint64")
		return typeid(uint64_t);
	else if (dtname == "uint16")
		return typeid(uint16_t);
	else
		abort();
	return typeid(nullptr);
}

static void validate_and_fully_parse_args(cmd_config_t &conf, cmd_args_t &args) {
	conf.in_filename = string(args.in_filename_str);
	conf.out_filename = string(args.out_filename_str);
	conf.datatype = datatype_name_to_type(args.datatype);
	conf.outdatatype = datatype_name_to_type(args.outdatatype);

	conf.equibin_nbins = args.equibin_nbins;
	conf.equibin_sampling_ratio = args.equibin_sampling_ratio;
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

template<typename WordT>
static void quantize_outword_switch(const cmd_config_t &conf) {
    if (*conf.outdatatype == typeid(double))
    	quantize_equalbinning<WordT, double>(conf);
    else if (*conf.outdatatype == typeid(uint64_t))
    	quantize_equalbinning<WordT, uint64_t>(conf);
    else if (*conf.outdatatype == typeid(uint16_t))
    	quantize_equalbinning<WordT, uint16_t>(conf);
    else {
    	std::cerr << "Invalid out datatype " << conf.outdatatype << std::endl;
    	abort();
    }
}

static void quantize_inword_switch(const cmd_config_t &conf) {
	if (*conf.datatype == typeid(double))
		quantize_outword_switch<double>(conf);
	else if (*conf.datatype == typeid(uint64_t))
		quantize_outword_switch<uint64_t>(conf);
	else if (*conf.datatype == typeid(uint16_t))
		quantize_outword_switch<uint16_t>(conf);
	else {
		std::cerr << "Invalid datatype " << conf.datatype << std::endl;
    	abort();
    }
}

int main(int argc, char **argv) {
    cmd_args_t args;
    cmd_config_t conf;

    // static bool FALSEVAL = false, TRUEVAL = true;

    myoption opts[] = {
        addopt("fin", required_argument, OPTION_TYPE_STRING, &args.in_filename_str),
        addopt("fout", required_argument, OPTION_TYPE_STRING, &args.out_filename_str),
        addopt("dt", required_argument, OPTION_TYPE_STRING, &args.datatype),
        addopt("odt", required_argument, OPTION_TYPE_STRING, &args.outdatatype),

        addopt("nbins", required_argument, OPTION_TYPE_INT, &args.equibin_nbins),
        addopt("sample", required_argument, OPTION_TYPE_INT, &args.equibin_sampling_ratio),

        addopt(NULL, required_argument, OPTION_TYPE_BOOLEAN, NULL),
    };
    parse_args(&argc, &argv, opts);
    validate_and_fully_parse_args(conf, args);

    quantize_inword_switch(conf);
}


