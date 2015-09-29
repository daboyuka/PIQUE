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
 * test-database-loadsave.cpp
 *
 *  Created on: Sep 23, 2014
 *      Author: David A. Boyuka II
 */

#include <iostream>
#include <string>

#include "pique/io/database.hpp"

static boost::shared_ptr< Database > create_database() {
	boost::shared_ptr< Database > db = boost::make_shared< Database >();

	db->add_variable("bob", boost::optional< std::string >("somewhere/bob.meta"), boost::optional< std::string >("somewhere/bob.idx"));
	db->add_variable("fred", boost::none, boost::optional< std::string >("somewhere/fred.idx"));
	db->add_variable("otto", boost::optional< std::string >("somewhere/otto.meta"), boost::none);

	return db;
}

static void write_and_verify_database(boost::shared_ptr< Database > db, std::string dbfilename) {
	assert(db->save_to_file(dbfilename));

	boost::shared_ptr< Database >loadeddb = Database::load_from_file(dbfilename);

	assert(loadeddb->get_num_variables() == 3);
	boost::shared_ptr< DataVariable > bobvar = loadeddb->get_variable("bob");
	boost::shared_ptr< DataVariable > fredvar = loadeddb->get_variable("fred");
	boost::shared_ptr< DataVariable > ottovar = loadeddb->get_variable("otto");
	assert(bobvar && fredvar && ottovar);

	assert(bobvar->get_dataset_meta_path() && *bobvar->get_dataset_meta_path() == "somewhere/bob.meta");
	assert(bobvar->get_index_path() && *bobvar->get_index_path() == "somewhere/bob.idx");
	assert(!fredvar->get_dataset_meta_path());
	assert(fredvar->get_index_path() && *fredvar->get_index_path() == "somewhere/fred.idx");
	assert(ottovar->get_dataset_meta_path() && *ottovar->get_dataset_meta_path() == "somewhere/otto.meta");
	assert(!ottovar->get_index_path());
}

int main(int argc, char **argv) {
	char *tempdir_cstr = std::getenv("testworkdir");
	std::string tempdir = (tempdir_cstr ? tempdir_cstr : "test.work.dir");
	std::string dbfilename = tempdir + "/database.dbmeta";

	boost::shared_ptr< Database > db = create_database();
	write_and_verify_database(db, dbfilename);
}
