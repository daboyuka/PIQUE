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

#include <boost/algorithm/string.hpp>

#include <pique/data/grid.hpp>
#include <pique/util/datatypes.hpp>

using namespace std;

struct cmd_args_t {
	char *dataset_filename_str;
	bool c_order;
	char *datatype_str;
	char *dims_str;
};

struct cmd_config_t {
	string dataset_filename;
	string datatype;
	Grid grid;
};

static void validate_and_fully_parse_args(cmd_config_t &conf, cmd_args_t &args) {
	conf.dataset_filename = string(args.dataset_filename_str);

	conf.datatype = string(args.datatype_str);

	vector<string> dim_tokens;
	string dims_str = string(args.dims_str);
	boost::algorithm::split(dim_tokens, dims_str, boost::algorithm::is_any_of(" ,"), boost::token_compress_on);

	Grid grid(Grid::Linearization::ROW_MAJOR_ORDER); // TODO: Ignores C vs. FORTRAN order (args.c_order); do we even need such a flag, though?
	for (auto it = dim_tokens.begin(); it != dim_tokens.end(); it++)
		grid.push_back(strtoll(it->c_str(), NULL, 0));

	conf.grid = grid;

	assert(Datatypes::get_datatypeid_by_name(conf.datatype) != Datatypes::IndexableDatatypeID::UNDEFINED);
	for (auto it = grid.begin(); it != grid.end(); it++)
		assert(*it > 0 /* All dimensions must be >0 */);
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

    bool TRUEVAL = true;

    myoption opts[] = {
        addopt("dataset", required_argument, OPTION_TYPE_STRING, &args.dataset_filename_str),
        addopt("datatype", required_argument, OPTION_TYPE_STRING, &args.datatype_str),
        addopt("dims", required_argument, OPTION_TYPE_STRING, &args.dims_str),
        addopt("corder", no_argument, OPTION_TYPE_BOOLEAN, &args.c_order, &TRUEVAL),
        addopt(NULL, required_argument, OPTION_TYPE_BOOLEAN, NULL),
    };
    parse_args(&argc, &argv, opts);
    validate_and_fully_parse_args(conf, args);

    cout << conf.dataset_filename << endl;
    cout << conf.datatype << " " << "C"; //(conf.grid->get_grid()->cOrder ? "C" : "FORTRAN"); // TODO: Ignores C vs. FORTRAN order; do we even need such a flag, though?
    for (auto it = conf.grid.begin(); it != conf.grid.end(); it++)
    	cout << " " << *it;
    cout << endl;
}
