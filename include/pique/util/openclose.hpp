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
 * openclose.hpp
 *
 *  Created on: Sep 4, 2014
 *      Author: David A. Boyuka II
 */
#ifndef OPENCLOSE_HPP_
#define OPENCLOSE_HPP_

template<typename... OpenArgsT>
class OpenableCloseable {
public:
	OpenableCloseable() : is_this_open(false) {}
	virtual ~OpenableCloseable() {}

	bool is_open() const { return is_this_open; }

	bool open(OpenArgsT... args) {
		if (is_this_open) return false;
		if (!this->open_impl(args...)) return false;
		is_this_open = true;
		return true;
	}
	bool close() {
		if (!is_this_open) return false;
		if (!this->close_impl()) return false;
		is_this_open = false;
		return true;
	}
private:
	virtual bool open_impl(OpenArgsT... args) = 0;
	virtual bool close_impl() = 0;

	bool is_this_open;
};

#endif /* OPENCLOSE_HPP_ */
