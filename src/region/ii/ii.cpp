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
 * ii.cpp
 *
 *  Created on: Feb 7, 2014
 *      Author: David A. Boyuka II
 */

#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/region/ii/ii.hpp"

using namespace std;

constexpr RegionEncoding::Type IIRegionEncoding::TYPE;

void IIRegionEncoding::dump() const {
	cout << "Domain size: " << this->domain_size << endl;

	int mod = 0;
	for (auto it = this->rids.begin(); it != this->rids.end(); it++) {
		cout << *it << " ";
		if ((++mod %= 16) == 0)
			cout << endl;
	}

	if (mod != 0)
		cout << endl;
}

bool IIRegionEncoding::operator==(const RegionEncoding &other_base) const {
	if (typeid(other_base) != typeid(IIRegionEncoding))
		return false;
	const IIRegionEncoding &other = dynamic_cast<const IIRegionEncoding&>(other_base);
	return this->domain_size == other.domain_size && this->rids == other.rids;
}
