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
 * write-dataset-metafile.hpp
 *
 *  Created on: Sep 8, 2014
 *      Author: David A. Boyuka II
 */
#ifndef WRITE_DATASET_METAFILE_HPP_
#define WRITE_DATASET_METAFILE_HPP_

#include <cassert>
#include <string>
#include <fstream>
#include <boost/smart_ptr.hpp>

#include "pique/util/datatypes.hpp"
#include "pique/data/grid.hpp"
#include "pique/data/dataset.hpp"

static void write_dataset_metadata_file(const InMemoryDataset<int> &dataset, std::string metadata_file = "dataset.meta", std::string dataset_path = "/invalid/path/here") {
	std::ofstream fout;
	fout.open(metadata_file, std::ios::out | std::ios::trunc);
	assert(fout.good());

	fout << dataset_path << std::endl;

	std::string datatype_name = *Datatypes::get_name_by_datatypeid(dataset.get_datatype());

	fout << datatype_name << " " <<
			"C"; //(dataset->get_grid()->get_grid()->cOrder ? "C" : "FORTRAN"); // TODO: ignores C vs. FORTRAN; do we even need this, though?
	for (uint64_t dim : dataset.get_grid())
		fout << " " << dim;
	fout << std::endl;

	fout.close();
}

#endif /* WRITE_DATASET_METAFILE_HPP_ */
