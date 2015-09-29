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
 * cache_ptr.hpp
 *
 *  Created on: Jan 7, 2015
 *      Author: David A. Boyuka II
 */
#ifndef CACHE_PTR_HPP_
#define CACHE_PTR_HPP_

#include <boost/smart_ptr.hpp>

namespace boost {

/*
 * The cache_ptr has the interface of a weak_ptr, but can be dynamically strengthen()ed
 * and weaken()ed to act as a shared_ptr or weak_ptr, respectively. The pointer assumes the
 * strength of any shared_ptr/weak_ptr assigned to it or that it is constructed with.
 * The current reference strength can be determined via is_strong().
 *
 * The main use of this pointer is in a cache. Cache pointers could initially be strong,
 * maintaining the cached values even when not in use. Later, if more memory is desired or
 * "stale" resources should be released, all pointers can be weakened. This may be superior
 * to simply using shared_ptrs, as it does not lose reference to resources that are currently
 * in use, and if all pointers are then re-strengthened, these in-use resources will again
 * be maintained beyond their current usage.
 */
template<typename T>
class cache_ptr : public boost::weak_ptr< T > {
private:
	typedef boost::weak_ptr< T > parent;
public:
	typedef T elememt_type;

	cache_ptr() : parent(), strongptr() {}
    cache_ptr(cache_ptr const & r) : parent(r), strongptr(r.strongptr) {}
	cache_ptr(std::nullptr_t) : parent(), strongptr() {}
    template<class U> cache_ptr(cache_ptr< U > const & r) : parent(r), strongptr(r.strongptr) {}
    template<class U> cache_ptr(shared_ptr< U > const & r) : parent(r), strongptr(r) {}
    template<class U> cache_ptr(weak_ptr< U > const & r) : parent(r), strongptr() {}

    cache_ptr & operator=(cache_ptr const & r) { this->parent::operator=(r); this->strongptr = r.strongptr; return *this; }
    cache_ptr & operator=(std::nullptr_t) { this->reset(); return *this; }
    template<class U> cache_ptr & operator=(weak_ptr< U > const & r) { parent::operator=(r); this->strongptr = nullptr; return *this; }
    template<class U> cache_ptr & operator=(shared_ptr< U > const & r) { parent::operator=(r); this->strongptr = r; return *this; }
    template<class U> cache_ptr & operator=(cache_ptr< U > const & r) { parent::operator=(r); this->strongptr = r.strongptr; return *this; }

    void swap(cache_ptr< T > & b) {
    	this->parent::swap(b);
    	std::swap(this->strong_ptr, b.strong_ptr);
    }

    void reset() {
    	this->parent::reset();
    	this->strongptr.reset();
    }

    void strengthen() { this->strongptr = this->lock(); }
    void weaken() { this->strongptr = nullptr; }
    /*
     * If is_strong(), temporarily weaken()s this pointer (destroying the referenced
     * resource if this is the last pointer), then re-strengthen()s it.
     * If !is_strong(), no effect.
     * Returns !is_expired() (i.e., true iff the referenced resource still exists).
     */
    bool release_unused() {
    	if (this->is_strong()) {
    		this->weaken();
    		this->strengthen();
    	}
    	return !this->expired();
    }
    bool is_strong() { return this->strongptr != nullptr; }

private:
    boost::shared_ptr< T > strongptr;
};

}; // namespace boost


template<class T, class U>
bool operator<(boost::cache_ptr< T > const & a, boost::cache_ptr< U > const & b) {
	return static_cast< boost::weak_ptr< T > const & >(a) < static_cast< boost::weak_ptr< U > const & >(b);
}

template<class T>
void swap(boost::cache_ptr< T > & a, boost::cache_ptr< T > & b) {
	swap(static_cast< boost::weak_ptr< T > const & >(a), static_cast< boost::weak_ptr< T > const & >(b));
}

#endif /* CACHE_PTR_HPP_ */
