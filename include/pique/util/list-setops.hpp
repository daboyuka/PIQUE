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
 * list-setops.hpp
 *
 *  Created on: Apr 7, 2014
 *      Author: drew
 */

#ifndef LIST_SETOPS_HPP_
#define LIST_SETOPS_HPP_

#include <vector>
#include "pique/setops/setops.hpp"

template<NArySetOperation setop, typename OutQueueT, typename rid_t>
class ListSetOperationsLoopBody {};

template<typename OutQueueT, typename rid_t>
class ListSetOperationsLoopBody<NArySetOperation::UNION, OutQueueT, rid_t> {
protected:
	inline void loop_body(int rid_comp, rid_t left_rid, rid_t right_rid, OutQueueT &out) {
		if (rid_comp <= 0)
			out.push_back(left_rid);
		else
			out.push_back(right_rid);
	}
};

template<typename OutQueueT, typename rid_t>
class ListSetOperationsLoopBody<NArySetOperation::INTERSECTION, OutQueueT, rid_t> {
protected:
	inline void loop_body(int rid_comp, rid_t left_rid, rid_t right_rid, OutQueueT &out) {
		if (rid_comp == 0)
			out.push_back(left_rid);
	}
};

template<typename OutQueueT, typename rid_t>
class ListSetOperationsLoopBody<NArySetOperation::DIFFERENCE, OutQueueT, rid_t> {
protected:
	inline void loop_body(int rid_comp, rid_t left_rid, rid_t right_rid, OutQueueT &out) {
		if (rid_comp == -1)
			out.push_back(left_rid);
	}
};

template<typename OutQueueT, typename rid_t>
class ListSetOperationsLoopBody<NArySetOperation::SYMMETRIC_DIFFERENCE, OutQueueT, rid_t> {
protected:
	inline void loop_body(int rid_comp, rid_t left_rid, rid_t right_rid, OutQueueT &out) {
		if (rid_comp == -1)
			out.push_back(left_rid);
		else if (rid_comp == 1)
			out.push_back(right_rid);
	}
};

template<NArySetOperation setop, typename LeftIterT, typename RightIterT, typename OutQueueT, typename rid_t>
struct ListSetOperationImpl : protected ListSetOperationsLoopBody<setop, OutQueueT, rid_t> {
	void list_set_operation(LeftIterT left_it, LeftIterT left_it_end, RightIterT right_it, RightIterT right_it_end, OutQueueT &out) {
		while (left_it != left_it_end && right_it != right_it_end) {
			const rid_t left_rid = *left_it;
			const rid_t right_rid = *right_it;
			int rid_comp;
			if (left_rid < right_rid) {
				rid_comp = -1;
				left_it++;
			} else if (left_rid > right_rid) {
				rid_comp = 1;
				right_it++;
			} else {
				rid_comp = 0;
				left_it++;
				right_it++;
			}

			this->loop_body(rid_comp, left_rid, right_rid, out);
		}

		// Append the remaining elements of the longer list, if the current operation dictates it
		if (left_it != left_it_end &&
				(setop == NArySetOperation::UNION ||
				 setop == NArySetOperation::DIFFERENCE ||
				 setop == NArySetOperation::SYMMETRIC_DIFFERENCE)) {
			out.insert(out.end(), left_it, left_it_end);
		}
		if (right_it != right_it_end &&
				(setop == NArySetOperation::UNION ||
				 setop == NArySetOperation::SYMMETRIC_DIFFERENCE)) {
			out.insert(out.end(), right_it, right_it_end);
		}
	}
};

template<typename LeftIterT, typename RightIterT, typename OutQueueT, typename rid_t>
void list_set_operation(NArySetOperation setop, LeftIterT left_it, LeftIterT left_it_end, RightIterT right_it, RightIterT right_it_end, OutQueueT &out) {
	switch (setop) {
	case NArySetOperation::UNION:
		ListSetOperationImpl<NArySetOperation::UNION, LeftIterT, RightIterT, OutQueueT, rid_t>()
			.list_set_operation(left_it, left_it_end, right_it, right_it_end, out);
		break;
	case NArySetOperation::INTERSECTION:
		ListSetOperationImpl<NArySetOperation::INTERSECTION, LeftIterT, RightIterT, OutQueueT, rid_t>()
			.list_set_operation(left_it, left_it_end, right_it, right_it_end, out);
		break;
	case NArySetOperation::DIFFERENCE:
		ListSetOperationImpl<NArySetOperation::DIFFERENCE, LeftIterT, RightIterT, OutQueueT, rid_t>()
			.list_set_operation(left_it, left_it_end, right_it, right_it_end, out);
		break;
	case NArySetOperation::SYMMETRIC_DIFFERENCE:
		ListSetOperationImpl<NArySetOperation::SYMMETRIC_DIFFERENCE, LeftIterT, RightIterT, OutQueueT, rid_t>()
			.list_set_operation(left_it, left_it_end, right_it, right_it_end, out);
		break;
	}
}

#endif /* LIST_SETOPS_HPP_ */
