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
 * region-encoding-impl.hpp
 *
 *  Created on: Feb 3, 2014
 *      Author: David A. Boyuka II
 */
#ifndef REGION_ENCODING_IMPL_HPP_
#define REGION_ENCODING_IMPL_HPP_

template<typename RE, typename REC>
void RegionEncoder<RE, REC>::push_bits(uint64_t count, bool bitval) {
	if (!count) return;
	this->push_bits_impl(count, bitval);
	current_bit_count += count;
}

template<typename RE, typename REC>
void RegionEncoder<RE, REC>::insert_bits(uint64_t position, uint64_t count) {
    assert(position >= this->current_bit_count);

	// First push 0's to fill the gap between the last pushed bit and the insert start position
	this->push_bits(position - this->current_bit_count, false);
	this->push_bits(count, true);
}

template<typename RE, typename REC>
void RegionEncoder<RE, REC>::finalize() {
    this->push_bits(this->total_bit_count - this->current_bit_count, false);
    this->finalize_impl();
}

#endif /* REGION_ENCODING_IMPL_HPP_ */
