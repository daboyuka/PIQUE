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
 * query.hpp
 *
 *  Created on: Apr 2, 2014
 *      Author: drew
 */

#ifndef QUERY_HPP_
#define QUERY_HPP_

#include <string>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/setops/setops.hpp"
#include "pique/io/database.hpp"
#include "pique/util/universal-value.hpp"

struct QueryTerm {
	virtual ~QueryTerm() {}
	virtual std::string to_string() = 0;
};

struct ConstraintTerm : public QueryTerm {
	ConstraintTerm(std::string varname, UniversalValue lower_bound, UniversalValue upper_bound) :
		varname(varname), lower_bound(lower_bound), upper_bound(upper_bound)
	{}

	virtual std::string to_string() { return lower_bound.to_string() + "<" + varname + "<" + upper_bound.to_string(); }

	std::string varname;
	UniversalValue lower_bound, upper_bound;
};

struct UnaryOperatorTerm : public QueryTerm {
	UnaryOperatorTerm(UnarySetOperation op) : op(op) {}

	virtual std::string to_string() {
		switch (op) {
		case UnarySetOperation::COMPLEMENT: return "NOT";
		default: abort(); return nullptr;
		}
	}

	UnarySetOperation op;
};

struct NAryOperatorTerm : public QueryTerm {
	NAryOperatorTerm(NArySetOperation op, int arity = 2) : op(op), arity(arity) {}

	virtual std::string to_string() {
		switch (op) {
		case NArySetOperation::INTERSECTION: return "AND";
		case NArySetOperation::UNION: return "OR";
		case NArySetOperation::DIFFERENCE: return "ANDNOT";
		case NArySetOperation::SYMMETRIC_DIFFERENCE: return "XOR";
		default: abort(); return nullptr;
		}
	}

	NArySetOperation op;
	int arity;
};

// Uses RPN, operator toward back end
class Query : public std::vector< boost::shared_ptr< QueryTerm > > {
public:
	Query() {}
};

Query operator&(const Query &left, const Query &right);
Query operator|(const Query &left, const Query &right);

#endif /* QUERY_HPP_ */
