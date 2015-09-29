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
 * zo-iter2.hpp
 *
 * TODO: Switch ZOIterLoopBody from std::function<> to function pointer,
 * templated/lambda functor, or other potentially-statically-optimizable
 * loop body form.
 *
 *  Created on: Apr 7, 2014
 *      Author: David A. Boyuka II
 */

#ifndef ZO_ITER2_HPP_
#define ZO_ITER2_HPP_

#include <boost/mpl/if.hpp>
#include <boost/mpl/comparison.hpp>
#include <functional>

typedef std::function<void(uint64_t,uint64_t,uint64_t[])> ZOIterLoopBody;

// NOTE: Dimensions are in FORTRAN order! dims[0] is the fastest-varying dimension
template<int ndim, int bitpos>
struct ZOIterLoopCore {
	static inline void iterate_core(uint64_t &zid, uint64_t &rmoid, uint64_t coords[ndim], const uint64_t dims[ndim], const uint64_t strides[ndim], ZOIterLoopBody &loop_body) {
		const int dimscale = bitpos / ndim;
		const int whichdim = bitpos % ndim;
		const uint64_t zid_mask = 1ULL << (bitpos);
		const uint64_t coord_mask = 1ULL << dimscale;

		uint64_t &coord = coords[whichdim];
		const uint64_t &dim = dims[whichdim];
		const uint64_t &stride = strides[whichdim];
		const uint64_t scaled_stride = stride << dimscale;

		do {
			if (coord < dim)
				ZOIterLoopCore<ndim, bitpos - 1>::iterate_core(zid, rmoid, coords, dims, strides, loop_body);

			zid ^= zid_mask;
			coord ^= coord_mask;
			rmoid += scaled_stride; // Overflows after the 2nd addition, however it's undone below
		} while (coord & coord_mask);

		rmoid -= scaled_stride << 1; // Undo the RMO index addition
	}
};

template<int ndim>
struct ZOIterLoopCore<ndim, 0> {
	static inline void iterate_core(uint64_t &zid, uint64_t &rmoid, uint64_t coords[ndim], const uint64_t dims[ndim], const uint64_t strides[ndim], ZOIterLoopBody &loop_body) {
		const uint64_t zid_mask = 1ULL;
		const uint64_t coord_mask = 1ULL;

		const int whichdim = 0;
		uint64_t &coord = coords[whichdim];
		const uint64_t &dim = dims[whichdim];
		const uint64_t &stride = strides[whichdim];

		//if (coord < dim) // Not needed; parent call already checked this (TODO: remove the first check from all iterations?)
			loop_body(zid, rmoid, coords);

		zid ^= zid_mask;
		coord ^= coord_mask;
		rmoid += stride;

		if (coord < dim)
			loop_body(zid, rmoid, coords);

		zid ^= zid_mask;
		coord ^= coord_mask;
		rmoid -= stride;
	}
};

template<int ndim, int bitpos>
struct ZOIterLoopCoreOverflow {
	static inline void iterate_core(uint64_t &zid, uint64_t &rmoid, uint64_t coords[ndim], const uint64_t dims[ndim], const uint64_t strides[ndim], ZOIterLoopBody &loop_body) {
		fprintf(stderr, "Error: attempt to execute %d-dimension Z-order loop with %d Z-order bits; 64 is maximum\n", ndim, bitpos+1);
		abort();
	}
};

template<int ndim, int exp_level>
class ZOIterLoop : private boost::mpl::if_<
					boost::mpl::less<boost::mpl::int_<exp_level * ndim>, boost::mpl::int_<sizeof(uint64_t)*8>>, // If there won't be a bit overflow...
					ZOIterLoopCore<ndim, exp_level * ndim - 1>,             // ...then inherit from the actual loop core; else,...
					ZOIterLoopCoreOverflow<ndim, exp_level * ndim - 1>      // ...inherit from code that will immediately abort
				   >::type {
public:
	inline void iterate(const uint64_t dims[ndim], const uint64_t subdims[ndim], bool cOrder, ZOIterLoopBody loop_body) {
		uint64_t zid = 0;
		uint64_t rmoid = 0;
		uint64_t coords[ndim] = {}; // All 0

		uint64_t dims_ftn[ndim];
		uint64_t subdims_ftn[ndim];
		uint64_t strides_ftn[ndim];
		for (int i = 0; i < ndim; i++) {
			int indim = cOrder ? ndim - i - 1 : i;

			dims_ftn[i] = dims[indim];
			subdims_ftn[i] = subdims[indim];
			strides_ftn[i] = i > 0 ? strides_ftn[i - 1] * dims_ftn[i - 1] : 1;
		}

		this->iterate_core(zid, rmoid, coords, subdims_ftn, strides_ftn, loop_body);
	}
};

template<int ndim>
inline void zo_loop_iterate(const uint64_t dims[], const uint64_t subdims[], bool cOrder, ZOIterLoopBody loop_body) {
	uint64_t maxdim = 0;
	for (int i = 0; i < ndim; ++i)
		if (maxdim < dims[i])
			maxdim = dims[i];

	int exp_level;
	for (exp_level = 0; maxdim > 1; ++exp_level, maxdim >>= 1);

	if (exp_level > 64)
		abort();
	else if (exp_level > 32)
		ZOIterLoop<ndim, 64>().iterate(dims, subdims, cOrder, loop_body);
	else if (exp_level > 16)
		ZOIterLoop<ndim, 32>().iterate(dims, subdims, cOrder, loop_body);
	else if (exp_level > 8)
		ZOIterLoop<ndim, 16>().iterate(dims, subdims, cOrder, loop_body);
	else if (exp_level > 4)
		ZOIterLoop<ndim, 8>().iterate(dims, subdims, cOrder, loop_body);
	else
		ZOIterLoop<ndim, 4>().iterate(dims, subdims, cOrder, loop_body);
}


inline void zo_loop_iterate(int ndim, const uint64_t dims[], const uint64_t subdims[], bool cOrder, ZOIterLoopBody loop_body) {
	switch (ndim) {
	case 1: zo_loop_iterate<1>(dims, subdims, cOrder, loop_body); return;
	case 2: zo_loop_iterate<2>(dims, subdims, cOrder, loop_body); return;
	case 3: zo_loop_iterate<3>(dims, subdims, cOrder, loop_body); return;
	case 4: zo_loop_iterate<4>(dims, subdims, cOrder, loop_body); return;
	case 5: zo_loop_iterate<5>(dims, subdims, cOrder, loop_body); return;
	}
}

#endif /* ZO_ITER2_HPP_ */
