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
 * index-io-cache.cpp
 *
 *  Created on: Jan 7, 2015
 *      Author: David A. Boyuka II
 */

#include "pique/io/index-io-cache.hpp"

void IndexIOCache::VarCache::release() {
	partitions.clear(); // First, close all child partition IOs (via destructors)
	this->indexio = nullptr;
}

void IndexIOCache::VarPartitionCache::release()  {
	this->partio = nullptr;
}

auto IndexIOCache::emplace_var_cache(std::string varname) -> VarCache & {
	return this->cache.emplace(varname, VarCache()).first->second;
}

auto IndexIOCache::VarCache::emplace_partition_cache(partition_id_t partition) -> VarPartitionCache & {
	return this->partitions.emplace(partition, VarPartitionCache()).first->second;
}

void IndexIOCache::release_var_cache(std::string varname) {
	this->cache.erase(varname);
}

void IndexIOCache::VarCache::release_partition_cache(partition_id_t partition) {
	this->partitions.erase(partition);
}



void IndexIOCache::release_all() {
	this->cache.clear();
}

void IndexIOCache::release_unused() {
	for (auto &entry : this->cache) {
		VarCache &varcache = entry.second;

		bool has_parts_open = false;
		for (auto &entry2 : varcache.partitions) {
			VarPartitionCache &partcache = entry2.second;
			partcache.partio.release_unused();
			if (!partcache.partio.expired() && partcache.partio.lock()->is_open())
				has_parts_open = true;
		}

		varcache.indexio.release_unused();

		if (varcache.indexio.expired() && has_parts_open) {
			std::cerr << "Invalid state: in IndexIOCache, an IndexIO was closed and destroyed, but some child IndexPartitionIOs are still open. "
					  << "Users of IndexIOCache must ensure parent IndexIOs are held via shared_ptr/strong cache_ptr while child IndexPartitionIOs are in use." << std::endl;
			abort();
		}
	}
}



boost::shared_ptr< IndexIO > IndexIOCache::create_new_index_io(const Database &database, std::string varname, IndexOpenMode mode) {
	return database.get_variable(varname)->open_index(mode);
}

boost::shared_ptr< IndexIO > IndexIOCache::open_index_io(std::string varname) {
	VarCache &varcache = this->emplace_var_cache(varname);

	if (varcache.indexio.expired())
		varcache.indexio = this->create_new_index_io(*this->database, varname, this->mode);

	return varcache.indexio.lock();
}

void IndexIOCache::release_index_io(std::string varname) {
	this->release_var_cache(varname);
}

boost::shared_ptr< IndexPartitionIO > IndexIOCache::open_index_partition_io(std::string varname, partition_id_t partition) {
	VarCache &varcache = this->emplace_var_cache(varname);
	VarPartitionCache &partcache = varcache.emplace_partition_cache(partition);

	if (partcache.partio.expired())
		partcache.partio = this->open_index_io(varname)->get_partition(partition);

	return partcache.partio.lock();
}

void IndexIOCache::release_index_partition_io(std::string varname, partition_id_t partition) {
	VarCache &varcache = this->emplace_var_cache(varname);
	varcache.release_partition_cache(partition);
}
