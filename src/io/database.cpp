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
 * database.cpp
 *
 *  Created on: May 13, 2014
 *      Author: David A. Boyuka II
 */

#include "config.h" // include Autoconf config.h to get HAVE_HDF5

#include <cstdint>
#include <string>
#include <iostream>
#include <fstream>

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/optional.hpp>
#include <boost/none.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "pique/data/grid.hpp"
#include "pique/io/database.hpp"
#include "pique/io/index-io.hpp"
#include "pique/io/posix/posix-index-io.hpp"
#include "pique/util/datatypes.hpp"

// Expected format:
// Line 1: path to dataset file
// Line 2: <datatype> { C | FORTRAN } <dim1> <dim2> ... <dimN>
bool DataVariable::cache_metadata() {
	if (!dataset_meta_path)
		return false;

	std::ifstream fin;
	fin.open(*dataset_meta_path, std::ios::in);
	if (!fin.good())
		return nullptr;

	this->cached_metadata = Metadata();

	fin >> this->cached_metadata->format >> std::ws; // Skip whitespace after the format string
	std::getline(fin, this->cached_metadata->filename);

	if (boost::iequals(this->cached_metadata->format, "raw"))
		raw_format_metadata_parse(fin);
	else if (boost::iequals(this->cached_metadata->format, "hdf5"))
		hdf5_format_metadata_parse(fin);
	else
		abort();

	fin.close();

	return this->cached_metadata->datatype != Datatypes::IndexableDatatypeID::UNDEFINED && this->cached_metadata->grid.size() && this->dataset; // Ensure the XXX_format_metadata_parse initialized everything else
}

void DataVariable::raw_format_metadata_parse(std::ifstream &fin) {
	std::string params_str;
	std::getline(fin, params_str);

	std::vector<std::string> param_fields;
	boost::split(param_fields, params_str, boost::is_any_of(" "), boost::token_compress_on);

	this->cached_metadata->datatype = *Datatypes::get_datatypeid_by_name(param_fields[0]);
	assert(this->cached_metadata->datatype != Datatypes::IndexableDatatypeID::UNDEFINED);

	// TODO: Ignores C vs. FORTRAN order; do we even need that dimension order flag in the file, though?
	//const bool cOrder = boost::iequals(param_fields[1], "C");
	Grid grid(Grid::Linearization::ROW_MAJOR_ORDER);

	for (size_t i = 2; i < param_fields.size(); i++) {
		const uint64_t dim = boost::lexical_cast<uint64_t>(param_fields[i]);
		assert(dim > 0);
		grid.push_back(dim);
	}

	this->cached_metadata->grid = std::move(grid);

	this->dataset = boost::make_shared< RawDataset >(this->cached_metadata->filename, this->cached_metadata->grid, this->cached_metadata->datatype);
}

void DataVariable::hdf5_format_metadata_parse(std::ifstream &fin) {
#ifdef HAVE_HDF5
	std::string varpath;
	std::getline(fin, varpath);

	boost::shared_ptr< DatasetHDF5 > dataset = boost::make_shared< DatasetHDF5 >(this->cached_metadata->filename, varpath);
	this->cached_metadata->datatype = dataset->get_datatype();
	this->cached_metadata->grid = dataset->get_grid();

	this->dataset = boost::make_shared< DatasetHDF5 >(this->cached_metadata->filename, varpath);
#else
	std::cerr << "Error: attempted to read an HDF5 dataset, but HDF5 support is not compiled in this build" << std::endl;
	abort();
#endif
}

boost::shared_ptr< Dataset > DataVariable::open_dataset() {
	cache_metadata();
	return this->dataset;
}

boost::shared_ptr< IndexIO > DataVariable::open_index(IndexOpenMode openmode) {
	if (!this->index_path)
		return nullptr;

	boost::shared_ptr< IndexIO > iio = boost::make_shared< POSIXIndexIO >();
	iio->open(*this->index_path, openmode);

	return iio;
}

boost::shared_ptr< DataVariable > Database::get_or_add_variable(std::string varname) {
	boost::shared_ptr< DataVariable > dv = this->get_variable(varname);
	if (!dv) {
		dv = boost::make_shared< DataVariable >(varname, boost::none, boost::none);
		this->add_variable(dv);
	}
	return dv;
}

static std::pair< std::string, std::string > split_at_first(const std::string &str, char c) {
	size_t pos = str.find(c);
	if (pos == std::string::npos)
		return std::make_pair(str, "");

	std::pair< std::string, std::string > ret;
	ret.first = str.substr(0, pos);
	ret.second = str.substr(pos + 1);
	return ret;
}

boost::shared_ptr< Database > Database::load_from_file(std::string filename) {
	std::fstream fin;
	fin.open(filename, std::ios::in);
	if (!fin.good())
		return nullptr;

	boost::shared_ptr< Database > db = boost::make_shared< Database >();

	while (!fin.eof() && !fin.fail()) {
		std::string line;
		std::getline(fin, line);
		boost::trim(line);

		// Skip empty and comment lines
		if (line == "" || line[0] == '#')
			continue;

		std::string key, value;
		std::tie(key, value) = split_at_first(line, '=');
		boost::trim(key);
		boost::trim(value);

		std::string varname, varprop;
		std::tie(varname, varprop) = split_at_first(key, '.');

		boost::shared_ptr< DataVariable > var = db->get_or_add_variable(varname);
		if (boost::iequals(varprop, "metapath") || boost::iequals(varprop, "metafile")) {
			var->dataset_meta_path = value;
		} else if (boost::iequals(varprop, "indexpath") || boost::iequals(varprop, "indexfile")) {
			var->index_path = value;
		} else {
			std::cerr << "Invalid property '" << varprop << "' for variable '" << varname << "' in database descriptor file '" << filename << "'" << std::endl;
			abort();
		}
	}

	fin.close();

	return (!fin.eof() && fin.fail()) ? nullptr : db;
}

bool Database::save_to_file(std::string filename) {
	std::fstream fout;
	fout.open(filename, std::ios::out);
	if (!fout.good())
		return false;

	for (boost::shared_ptr< DataVariable > var : this->vars) {
		if (var->dataset_meta_path)
			fout << var->name << ".metapath=" << *var->dataset_meta_path << std::endl;
		if (var->index_path)
			fout << var->name << ".indexpath=" << *var->index_path << std::endl;
	}

	fout.close();
	return !fout.fail();
}
