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

#include <iostream>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <set>
#include <map>

#include <boost/algorithm/string.hpp>

using namespace std;

enum struct QuantizeMode { TRUNCATE, CONV_1C_TO_2C, ZERO_INSIGBITS};

struct cmd_args_t {
	char *in_filename_str;
	char *out_filename_str;
	int datatype_size;
	int outdatatype_size;
	int sigbits;
	char *quantize_mode;
	bool as_bids;
};

struct cmd_config_t {
	string in_filename;
	string out_filename;
	int datatype_size;
	int outdatatype_size;
	int sigbits;
	QuantizeMode mode;
	bool as_bids;
};

template<typename WordT, typename OutWordT>
static void quantize_binids(const cmd_config_t &conf) {
	const int wordbits = (sizeof(WordT) << 3);
	const int insigbits = wordbits - conf.sigbits;

	FILE *fin = std::fopen(conf.in_filename.c_str(), "r");
	FILE *fout = std::fopen(conf.out_filename.c_str(), "w");
	assert(fin && fout);

	std::set< WordT > keys;

	const uint64_t BUFFER_SIZE = 1048576;
	std::vector<WordT> buf(BUFFER_SIZE);

	while (!feof(fin)) {
		const size_t nwords = fread(&buf.front(), sizeof(WordT), buf.size(), fin);
		switch (conf.mode) {
		case QuantizeMode::TRUNCATE:
			for (size_t i = 0; i < nwords; i++)
				keys.insert(buf[i] >> insigbits);
			break;
		case QuantizeMode::CONV_1C_TO_2C:
		{
			const WordT signbitmask = (WordT)1 << (wordbits - 1);
			for (size_t i = 0; i < nwords; i++) {
				// 0XXXXXXXX -> 1XXXXXXXX
				// 1XXXXXXXX -> 0~(XXXXXXXX)
				if (buf[i] & signbitmask) {
					keys.insert((WordT)(~buf[i]) >> insigbits);
				} else {
					keys.insert((buf[i] | signbitmask) >> insigbits);
				}
			}
			break;
		}
		case QuantizeMode::ZERO_INSIGBITS:
		{
			const WordT keepmask = (~(WordT)0) << insigbits;
			for (size_t i = 0; i < nwords; i++)
				keys.insert(buf[i] & keepmask);
		}
		}
	}

	fseek(fin, 0, SEEK_SET);

	OutWordT nextbid = 0;
	std::map< WordT, OutWordT > bin_id_map;
	for (auto it = keys.begin(); it != keys.end(); it++)
		bin_id_map.insert(std::make_pair(*it, nextbid++));

	std::vector<OutWordT> outbuf(BUFFER_SIZE);

	while (!feof(fin)) {
		const size_t nwords = fread(&buf.front(), sizeof(WordT), buf.size(), fin);
		switch (conf.mode) {
		case QuantizeMode::TRUNCATE:
			for (size_t i = 0; i < nwords; i++)
				outbuf[i] = bin_id_map[buf[i] >> insigbits];
			break;
		case QuantizeMode::CONV_1C_TO_2C:
		{
			const WordT signbitmask = (WordT)1 << (wordbits - 1);
			for (size_t i = 0; i < nwords; i++) {
				// 0XXXXXXXX -> 1XXXXXXXX
				// 1XXXXXXXX -> 0~(XXXXXXXX)
				if (buf[i] & signbitmask) {
					outbuf[i] = bin_id_map[(WordT)(~buf[i]) >> insigbits];
				} else {
					outbuf[i] = bin_id_map[(buf[i] | signbitmask) >> insigbits];
				}
			}
			break;
		}
		case QuantizeMode::ZERO_INSIGBITS:
		{
			const WordT keepmask = (~(WordT)0) << insigbits;
			for (size_t i = 0; i < nwords; i++)
				outbuf[i] = bin_id_map[buf[i] & keepmask];
		}
		}
		fwrite(&outbuf.front(), sizeof(OutWordT), nwords, fout);
	}

	fclose(fin);
	fclose(fout);
}

template<typename WordT, typename OutWordT>
static void quantize(const cmd_config_t &conf) {
	if (conf.as_bids) {
		quantize_binids<WordT, OutWordT>(conf);
		return;
	}

	const int wordbits = (sizeof(WordT) << 3);
	const int insigbits = wordbits - conf.sigbits;

	FILE *fin = std::fopen(conf.in_filename.c_str(), "r");
	FILE *fout = std::fopen(conf.out_filename.c_str(), "w");
	assert(fin && fout);

	const uint64_t BUFFER_SIZE = 1048576;
	std::vector<WordT> buf(BUFFER_SIZE);
	std::vector<OutWordT> outbuf(BUFFER_SIZE);

	while (!feof(fin)) {
		const size_t nwords = fread(&buf.front(), sizeof(WordT), buf.size(), fin);
		switch (conf.mode) {
		case QuantizeMode::TRUNCATE:
			std::cerr << "TRUNCATE MODE" << std::endl;
			for (size_t i = 0; i < nwords; i++)
				outbuf[i] = (OutWordT)(buf[i] >> insigbits);
			break;
		case QuantizeMode::CONV_1C_TO_2C:
		{
			std::cerr << "1's TO 2's COMPLEMENT CONVERT MODE" << std::endl;
			const WordT signbitmask = (WordT)1 << (wordbits - 1);
			for (size_t i = 0; i < nwords; i++) {
				if (buf[i] & signbitmask) {
					const WordT rmsignbit = buf[i] ^ signbitmask;
					outbuf[i] = ~(OutWordT)(rmsignbit >> insigbits) + 1;
				} else {
					outbuf[i] = (OutWordT)(buf[i] >> insigbits);
				}
			}
			break;
		}
		case QuantizeMode::ZERO_INSIGBITS:
		{
			std::cerr << "ZERO INSIGBITS MODE" << std::endl;
			assert(typeid(WordT) == typeid(OutWordT));
			const WordT keepmask = (~(WordT)0) << insigbits;
			for (size_t i = 0; i < nwords; i++)
				outbuf[i] = (OutWordT)(buf[i] & keepmask);
		}
		}
		fwrite(&outbuf.front(), sizeof(OutWordT), nwords, fout);
	}

	fclose(fin);
	fclose(fout);
}

static void validate_and_fully_parse_args(cmd_config_t &conf, cmd_args_t &args) {
	conf.in_filename = string(args.in_filename_str);
	conf.out_filename = string(args.out_filename_str);
	conf.datatype_size = args.datatype_size;
	conf.outdatatype_size = args.outdatatype_size;
	conf.sigbits = args.sigbits;
	conf.as_bids = args.as_bids;

	if (strcasecmp(args.quantize_mode, "truncate") == 0)
		conf.mode = QuantizeMode::TRUNCATE;
	else if (strcasecmp(args.quantize_mode, "conv2c") == 0)
		conf.mode = QuantizeMode::CONV_1C_TO_2C;
	else if (strcasecmp(args.quantize_mode, "zero") == 0)
		conf.mode = QuantizeMode::ZERO_INSIGBITS;
	else
		abort();
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
    switch (conf.outdatatype_size) {
    case 1: quantize<WordT, uint8_t>(conf); break;
    case 2: quantize<WordT, uint16_t>(conf); break;
    case 4: quantize<WordT, uint32_t>(conf); break;
    case 8: quantize<WordT, uint64_t>(conf); break;
    default:
    	std::cout << "Invalid out datatype size " << conf.outdatatype_size << std::endl;
    	abort();
    }
}

static void quantize_inword_switch(const cmd_config_t &conf) {
    switch (conf.datatype_size) {
    case 1: quantize_outword_switch<uint8_t>(conf); break;
    case 2: quantize_outword_switch<uint16_t>(conf); break;
    case 4: quantize_outword_switch<uint32_t>(conf); break;
    case 8: quantize_outword_switch<uint64_t>(conf); break;
    default:
    	std::cout << "Invalid datatype size " << conf.datatype_size << std::endl;
    	abort();
    }
}

int main(int argc, char **argv) {
    cmd_args_t args;
    cmd_config_t conf;

    static bool FALSEVAL = false; // , TRUEVAL = true;
    static char DEF_QUANTIZE_MODE[] = "truncate";

    myoption opts[] = {
        addopt("fin", required_argument, OPTION_TYPE_STRING, &args.in_filename_str),
        addopt("fout", required_argument, OPTION_TYPE_STRING, &args.out_filename_str),
        addopt("dtsize", required_argument, OPTION_TYPE_INT, &args.datatype_size),
        addopt("odtsize", required_argument, OPTION_TYPE_INT, &args.outdatatype_size),
        addopt("sigbits", required_argument, OPTION_TYPE_INT, &args.sigbits),
        addopt("quantmode", required_argument, OPTION_TYPE_STRING, &args.quantize_mode, DEF_QUANTIZE_MODE),
        addopt("as_bids", required_argument, OPTION_TYPE_BOOLEAN, &args.as_bids, &FALSEVAL),
        addopt(NULL, required_argument, OPTION_TYPE_BOOLEAN, NULL),
    };
    parse_args(&argc, &argv, opts);
    validate_and_fully_parse_args(conf, args);

    assert(conf.sigbits >= 1 && conf.sigbits <= (conf.datatype_size << 3));

    quantize_inword_switch(conf);
}


