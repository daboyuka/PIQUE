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
 * index-io-cache.hpp
 *
 *  Created on: Jan 7, 2015
 *      Author: David A. Boyuka II
 */
#ifndef INDEX_IO_CACHE_HPP_
#define INDEX_IO_CACHE_HPP_

#include <string>
#include <unordered_map>
#include <boost/smart_ptr.hpp>

#include "pique/util/cache-ptr.hpp"
#include "pique/io/database.hpp"
#include "pique/io/index-io.hpp"

/*
 * Implements a cache for IndexIOs and IndexPartitionIOs, opening them as needed and closing them when cleaned or purged.
 * Therefore, user code should NOT manually close IndexIOs or IndexPartitionIOs when using this class, instead opting to
 * simply reset() or destroy the returned shared_ptrs.
 *
 * Internally uses strong references to maintain cached Index[Partition]IO. Calling release_all() will immediately release all
 * references, destroying any resources not in use elsewhere. Alternatively, calling release_unused() also immediately
 * releases all references, but then re-acquires those references that are in use elsewhere (that is, only those resources
 * that are not in use elsewhere are permanently released, and consequently destroyed).
 *
 * The default implementation opens new IndexIOs as POSIXIndexIOs. Derived classes may change this behavior by overriding
 * the virtual create_new_index_io() function.
 */
class IndexIOCache {
public:
	using partition_id_t = IndexIOTypes::partition_id_t;

public:
	IndexIOCache(boost::shared_ptr< Database > database, IndexOpenMode mode = IndexOpenMode::READ) : database(std::move(database)), mode(mode) {}
	virtual ~IndexIOCache() { this->release_all(); }

	boost::shared_ptr< Database > get_database() { return database; }

	void release_all();
	void release_unused();

	boost::shared_ptr< IndexIO > open_index_io(std::string varname);
	void release_index_io(std::string varname);

	boost::shared_ptr< IndexPartitionIO > open_index_partition_io(std::string varname, partition_id_t partition);
	void release_index_partition_io(std::string varname, partition_id_t partition);

private:
	// Delegate for derived classes to override
	virtual boost::shared_ptr< IndexIO > create_new_index_io(const Database &database, std::string varname, IndexOpenMode mode);

private:
	struct VarPartitionCache {
		~VarPartitionCache() { this->release(); }
		void release();

		boost::cache_ptr< IndexPartitionIO > partio;
	};

	struct VarCache {
		~VarCache() { this->release(); }
		void release();

		VarPartitionCache & emplace_partition_cache(partition_id_t partition);
		void release_partition_cache(partition_id_t partition);

		boost::cache_ptr< IndexIO > indexio;
		std::unordered_map< partition_id_t, VarPartitionCache > partitions;
	};

	VarCache & emplace_var_cache(std::string varname);
	void release_var_cache(std::string varname);

private:
	const boost::shared_ptr< Database > database;
	const IndexOpenMode mode;
	std::unordered_map< std::string, VarCache > cache;
};

#endif /* INDEX_IO_CACHE_HPP_ */
