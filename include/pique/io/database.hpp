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
 * database.hpp
 *
 *  Created on: Apr 2, 2014
 *      Author: drew
 */

#ifndef DATABASE_HPP_
#define DATABASE_HPP_

#include <algorithm>
#include <vector>
#include <typeindex>

#include <boost/none.hpp>
#include <boost/optional.hpp>
#include <boost/smart_ptr.hpp>

#include "pique/data/grid.hpp"
#include "pique/data/dataset.hpp"

// Use forward declaration instead of including the whole index-io.hpp
//#include "pique/io/index-io.hpp"
class IndexIO;
enum struct IndexOpenMode;

class DataVariable {
private:
	struct Metadata {
		Metadata() : filename(), format(), datatype(Datatypes::IndexableDatatypeID::UNDEFINED), grid() {}
		std::string filename;
		std::string format;
		Datatypes::IndexableDatatypeID datatype;
		Grid grid;
	};

public:
	DataVariable(std::string name, boost::optional<std::string> dataset_meta_path, boost::optional<std::string> index_path) :
		varid(-1), name(name), dataset_meta_path(dataset_meta_path), index_path(index_path)
	{}

	int get_varid() const { return varid; }
	std::string get_name() const { return name; }
	boost::optional< std::string > get_dataset_meta_path() { return dataset_meta_path; }
	boost::optional< std::string > get_index_path() { return index_path; }

	boost::optional< Datatypes::IndexableDatatypeID > get_datatype() {
		if (!cache_metadata() || cached_metadata->datatype != Datatypes::IndexableDatatypeID::UNDEFINED)
			return boost::none;
		return boost::optional< Datatypes::IndexableDatatypeID >(cached_metadata->datatype);
	}
	Grid get_grid() {
		cache_metadata();
		return cached_metadata->grid;
	}

	boost::shared_ptr< Dataset > open_dataset();
	boost::shared_ptr< IndexIO > open_index(IndexOpenMode openmode);

private:
	bool cache_metadata();

	void raw_format_metadata_parse(std::ifstream &fin);
	void hdf5_format_metadata_parse(std::ifstream &fin);

private:
	int varid;
	std::string name;
	boost::optional<std::string> dataset_meta_path;
	boost::optional<std::string> index_path;

	boost::optional< Metadata > cached_metadata;
	boost::shared_ptr< Dataset > dataset;

	friend class Database;
};

class Database {
public:
	int get_num_variables() const { return vars.size(); }
	std::vector< std::string > get_variable_names() const {
		std::vector< std::string > names;
		for (auto it = vars.cbegin(); it != vars.cend(); it++)
			names.push_back((*it)->name);
		return names;
	}

	const std::vector< boost::shared_ptr< DataVariable > > get_variables() const { return vars; }
	boost::shared_ptr< DataVariable > get_variable(int varid) const { return vars.at(varid); }

	boost::shared_ptr< DataVariable > get_variable(std::string varname) const {
		auto it = std::find_if(
					vars.begin(), vars.end(),
					[varname](const boost::shared_ptr< DataVariable > ptr)->bool {
						return ptr->get_name() == varname;
					});
		return it != vars.end() ? *it : nullptr;
	}

	int add_variable(boost::shared_ptr< DataVariable > var) {
		const int varid = vars.size();

		var->varid = varid;
		vars.push_back(var);
		return varid;
	}

	int add_variable(std::string name, boost::optional<std::string> dataset_meta_path = boost::none, boost::optional<std::string> index_path = boost::none) {
		return this->add_variable(boost::make_shared< DataVariable >(name, dataset_meta_path, index_path));
	}

	static boost::shared_ptr< Database > load_from_file(std::string filename);
	bool save_to_file(std::string filename);

private:
	boost::shared_ptr< DataVariable > get_or_add_variable(std::string varname);

private:
	std::vector< boost::shared_ptr< DataVariable > > vars;
};

#endif /* DATABASE_HPP_ */
