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
 * tableflip.cpp
 *
 *  Created on: Feb 3, 2015
 *      Author: David A. Boyuka II
 */

extern "C" {
#include <unistd.h>
}

#include <iostream>
#include <string>
#include <vector>
#include <set>
#include <map>

#include <boost/optional.hpp>
#include <boost/none.hpp>
#include <boost/algorithm/string.hpp>

typedef unsigned long index_t;
typedef std::pair< index_t, std::string > named_field_t;

struct cmd_config_t {
	std::vector< named_field_t > row_fields;
	std::vector< named_field_t > column_fields;
	std::vector< named_field_t > body_fields;
};

/*
static boost::optional< unsigned long > strtoul_checked(const char *str, int radix = 0) {
	char *endptr;
	const unsigned long ret = strtoul(str, &endptr, radix);

	if (endptr == str)
		return boost::none;
	else
		return boost::make_optional(ret);
}
*/

static boost::optional< cmd_config_t > parse_options(int &argc, char **&argv) {
	cmd_config_t conf;

	index_t cur_idx = 0;
	int c;
	while ((c = getopt(argc, argv, "b:c:r:")) != -1) {
		switch (c) {
		case 'c':
		case 'r':
		case 'b':
		{
			const std::string fieldname = optarg ? optarg : "";
			const named_field_t field = std::make_pair(cur_idx, fieldname);

			if (c == 'c')		conf.column_fields.push_back(field);
			else if (c == 'r')	conf.row_fields.push_back(field);
			else if (c == 'b')	conf.body_fields.push_back(field);
			else				abort();
			break;
		}
		case '?':
		default:
			abort();
		}

		++cur_idx;
	}

	if (conf.row_fields.empty()) {
		std::cerr << "Error: at least one row field needed" << std::endl;
		return boost::none;
	}
	if (conf.column_fields.empty()) {
		std::cerr << "Error: at least one column field needed" << std::endl;
		return boost::none;
	}
	if (conf.body_fields.empty()) {
		std::cerr << "Error: at least one body field needed" << std::endl;
		return boost::none;
	}

	argc -= optind; argv += optind;
	return boost::make_optional(conf);
}

typedef std::vector< std::string > dyntuple_t;
static inline bool operator<(const dyntuple_t &l, const dyntuple_t &r) {
	size_t minlen = std::min(l.size(), r.size());

	for (size_t i = 0; i < minlen; ++i) {
		if (l[i] < r[i])
			return true;
		else if (l[i] > r[i])
			return false;
	}

	// If they compare equal up to minlen, l < r iff l.size() < r.size()
	return l.size() < minlen;
}

int main(int argc, char **argv) {
	static constexpr size_t MAX_LINE_LENGTH = 16384;

	boost::optional< cmd_config_t > conf_opt = parse_options(argc, argv);
	if (!conf_opt) return -1;
	cmd_config_t &conf = *conf_opt;

	std::set< dyntuple_t > column_keys;
	std::set< dyntuple_t > row_keys;
	std::map< std::pair< dyntuple_t, dyntuple_t >, dyntuple_t > records;

	std::string line(MAX_LINE_LENGTH, '\0');
	dyntuple_t row, col, body;
	while (std::cin.getline(&line[0], MAX_LINE_LENGTH)) {
		const size_t linelen = std::cin.gcount();
	    line.resize(linelen);
	    boost::algorithm::trim(line);

	    std::vector< std::string > tokens;
	    boost::split(tokens, line, boost::is_any_of(" \t"), boost::token_compress_on);

	    row.clear(); col.clear(); body.clear();
	    for (named_field_t f : conf.column_fields)
	    	col.push_back(tokens[f.first]);
	    for (named_field_t f : conf.row_fields)
	    	row.push_back(tokens[f.first]);
	    for (named_field_t f : conf.body_fields)
	   		body.push_back(tokens[f.first]);

	    column_keys.insert(col);
	    row_keys.insert(row);
	    records.insert(
	    	std::make_pair(
	    		std::make_pair(row, col),
	    		body
	    	)
	    );

	    line.resize(MAX_LINE_LENGTH);
	}

	// First, print the column towers, along with column labels
	for (size_t col_key_pos = 0; col_key_pos < conf.column_fields.size(); ++col_key_pos) {
		for (size_t i = 0; i < conf.row_fields.size() - 1; ++i)
			std::cout << "\t";

		std::cout << conf.column_fields[col_key_pos].second << "\t";

		bool first = true;
		for (dyntuple_t col : column_keys) {
			if (!first) {
				for (size_t i = 0; i < conf.body_fields.size(); ++i)
					std::cout << "\t";
			}
			first = false;

			std::cout << col[col_key_pos];
		}

		std::cout<< std::endl;
	}

	// Next, print row and body value headers
	{
		bool first = true;
		for (const named_field_t &row_field : conf.row_fields) {
			if (!first)
				std::cout << "\t";
			first = false;
			std::cout << row_field.second;
		}
	}
	for (size_t col_keys = 0; col_keys < column_keys.size(); ++col_keys) {
		for (const named_field_t &body_field : conf.body_fields) {
			std::cout << "\t" << body_field.second;
		}
	}
	std::cout << std::endl;

	// Finally, print all rows
	for (dyntuple_t row : row_keys) {
		bool first = true;

		for (std::string tok : row) {
			if (!first) { std::cout << "\t"; }
			first = false;
			std::cout << tok;
		}

		for (dyntuple_t col : column_keys) {
			auto it = records.find(std::make_pair(row, col));
			if (it != records.end()) {
				for (std::string tok : it->second)
					std::cout << "\t" << tok;
			} else {
				for (size_t i = 0; i < conf.body_fields.size(); ++i)
					std::cout << "\t";
			}
		}

		std::cout << std::endl;
	}
}
