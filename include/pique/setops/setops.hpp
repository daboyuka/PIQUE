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
 * setops.hpp
 *
 *  Created on: Mar 19, 2014
 *      Author: David A. Boyuka II
 */
#ifndef SETOPS_HPP_
#define SETOPS_HPP_

#include <iterator>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/iterator/transform_iterator.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/util/variant-is-type.hpp"

enum struct UnarySetOperation { COMPLEMENT };
enum struct NArySetOperation { UNION, INTERSECTION, DIFFERENCE, SYMMETRIC_DIFFERENCE };

constexpr bool is_set_op_symmetric(NArySetOperation op) { return op != NArySetOperation::DIFFERENCE; } // "symmetry" means the order of all operands does not matter
constexpr bool is_set_op_tail_symmetric(NArySetOperation op) { return true; } // "tail symmetry" means that the order of operands 2 through N does not matter (but operand 1 may need to come first)

class SimplifiedSetOp {
public:
	using RegionUniformity = RegionEncoding::RegionUniformity;

	SimplifiedSetOp(NArySetOperation op, bool complement_op_result = false) :
		result_uniformity(RegionUniformity::MIXED), op(op), complement_op_result(complement_op_result) {}
	SimplifiedSetOp(RegionUniformity result_uniformity, NArySetOperation op = NArySetOperation(), bool complement_op_result = false) :
		result_uniformity(result_uniformity), op(op), complement_op_result(complement_op_result) {}

	SimplifiedSetOp append_operand(RegionUniformity operand_uniformity) const;

	static SimplifiedSetOp simplify(NArySetOperation op, const std::vector< RegionUniformity > &operand_uniformities);

	void finalize() {
		if (this->complement_op_result && this->result_uniformity != RegionUniformity::MIXED) {
			this->complement_op_result = !this->complement_op_result;
			this->result_uniformity = RegionEncoding::complement(this->result_uniformity);
		}
	}

	bool operator==(const SimplifiedSetOp &other) {
		// If the region uniformity of either SSO is not mixed, the operand is irrelevant
		if (this->result_uniformity != RegionUniformity::MIXED || other.result_uniformity != RegionUniformity::MIXED)
			return
				this->result_uniformity == other.result_uniformity &&
				this->complement_op_result == other.complement_op_result;
		else
			return
				this->result_uniformity == other.result_uniformity &&
				this->op == other.op &&
				this->complement_op_result == other.complement_op_result;
	}

public:
	RegionUniformity result_uniformity;
	NArySetOperation op;
	bool complement_op_result;
};



// Runtime type-dynamic set operations
class AbstractSetOperations {
public:
	virtual ~AbstractSetOperations() {}

public:
	using REPtr = boost::shared_ptr< RegionEncoding >;
	using RECPtr = boost::shared_ptr< const RegionEncoding >;

	virtual bool can_handle_region_encoding(RegionEncoding::Type type) const = 0;

	REPtr dynamic_unary_set_op(RECPtr region, UnarySetOperation op) const;
	REPtr dynamic_inplace_unary_set_op(REPtr region_and_out, UnarySetOperation op) const; // returns region_and_out

	REPtr dynamic_binary_set_op(RECPtr left, RECPtr right, NArySetOperation op) const;
	REPtr dynamic_inplace_binary_set_op(REPtr left_and_out, RECPtr right, NArySetOperation op) const; // returns left_and_out

	// Non-const operands
	typedef typename std::vector< REPtr >::const_iterator RegionEncodingPtrCIter;
	REPtr dynamic_nary_set_op(RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const; // delegates to dynamic_nary_set_op(const iters) // returns const RE because of the 1-operand case; if non-const is needed, use an inplace variant
	REPtr dynamic_inplace_nary_set_op(REPtr first_and_out, RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const; // delegates to dynamic_inplace_nary_set_op(const iters) // returns first_and_out
	REPtr dynamic_inplace_nary_set_op(RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const; // delegates to dynamic_inplace_nary_set_op(*it, ++it, end_it, op) // returns first_and_out

	// Const operands (except possibly the first)
	typedef typename std::vector< RECPtr >::const_iterator RegionEncodingCPtrCIter;
	RECPtr dynamic_nary_set_op(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const; // delegates to dynamic_nary_set_op_impl
	REPtr dynamic_nary_set_op(REPtr first, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const; // delegates to dynamic_nary_set_op_impl
	REPtr dynamic_inplace_nary_set_op(REPtr first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const; // delegates to dynamic_inplace_nary_set_op_impl // returns first_and_out

private:
	virtual REPtr dynamic_unary_set_op_impl(RECPtr region, UnarySetOperation op) const = 0;
	virtual void dynamic_inplace_unary_set_op_impl(REPtr region_and_out, UnarySetOperation op) const = 0;

	virtual REPtr dynamic_binary_set_op_impl(RECPtr left, RECPtr right, NArySetOperation op) const = 0;
	virtual void dynamic_inplace_binary_set_op_impl(REPtr left_and_out, RECPtr right, NArySetOperation op) const = 0;

	// Guaranteed to receive two or more operands
	virtual REPtr dynamic_nary_set_op_impl(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const = 0;

	// Guaranteed to receive two or more operands (including first_and_out)
	virtual void dynamic_inplace_nary_set_op_impl(REPtr first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const = 0;

	// Friendships needed so utility subclasses can call dynamic_nary_set_op_impl directly, as they needs the non-const return type when using two pointer-to-const iterators, and can guarantee at least two operands
	friend class PreferenceListSetOperations;
	friend class ArityThresholdSetOperations;
};



template<typename RegionEncodingT>
class SetOperations : public AbstractSetOperations {
public:
	typedef RegionEncodingT RegionEncodingType;

public:
	SetOperations() {}
	virtual ~SetOperations() {}

	virtual bool can_handle_region_encoding(RegionEncoding::Type type) const final;

public: // Extended API
	using REPtr = boost::shared_ptr< RegionEncoding >;
	using RECPtr = boost::shared_ptr< const RegionEncoding >;
	using REPtrT = boost::shared_ptr< RegionEncodingT >;
	using RECPtrT = boost::shared_ptr< const RegionEncodingT >;

	REPtrT unary_set_op(RECPtrT region, UnarySetOperation op) const;
    REPtrT inplace_unary_set_op(REPtrT region_and_out, UnarySetOperation op) const;

    REPtrT binary_set_op(RECPtrT left, RECPtrT right, NArySetOperation op) const;
    REPtrT inplace_binary_set_op(REPtrT left_and_out, RECPtrT right, NArySetOperation op) const;

    // Non-const operands
	typedef typename std::vector< REPtrT >::const_iterator RegionEncodingPtrCIter;
	REPtrT nary_set_op(RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const; // -> nary_set_op(REPtr, RECPtrCIters)
	REPtrT inplace_nary_set_op(RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const; // -> inplace_nary_set_op(REPtr, REPtrCIters)
	REPtrT inplace_nary_set_op(REPtrT first_and_out, RegionEncodingPtrCIter it, RegionEncodingPtrCIter end_it, NArySetOperation op) const; // -> inplace_nary_set_op(REPtr, RECPtrCIters)

	// Const operands (except possibly the first)
    typedef typename std::vector< RECPtrT >::const_iterator RegionEncodingCPtrCIter;
    RECPtrT nary_set_op(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const; // Delegates to nary_set_op_impl
    REPtrT  nary_set_op(REPtrT first, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const; // Delegates to nary_set_op_impl
    REPtrT  inplace_nary_set_op(REPtrT first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const; // Delegates to inplace_nary_set_op_impl

private:	// AbstractSetOperation impl. stub overrides
	virtual REPtr dynamic_unary_set_op_impl(RECPtr region, UnarySetOperation op) const final; // delegates to unary_set_op_impl
	virtual void dynamic_inplace_unary_set_op_impl(REPtr region_and_out, UnarySetOperation op) const final; // delegates to inplace_unary_set_op_impl

	virtual REPtr dynamic_binary_set_op_impl(RECPtr left, RECPtr right, NArySetOperation op) const final; // delegates to binary_set_op_impl
	virtual void dynamic_inplace_binary_set_op_impl(REPtr left_and_out, RECPtr right, NArySetOperation op) const final; // delegates to inplace_binary_set_op_impl

	using GRegionEncodingCPtrCIter = AbstractSetOperations::RegionEncodingCPtrCIter;
	virtual REPtr dynamic_nary_set_op_impl(GRegionEncodingCPtrCIter it, GRegionEncodingCPtrCIter end_it, NArySetOperation op) const final; // delegates to nary_set_op_impl
	virtual void dynamic_inplace_nary_set_op_impl(REPtr first_and_out, GRegionEncodingCPtrCIter it, GRegionEncodingCPtrCIter end_it, NArySetOperation op) const final; // delegates to inplace_nary_set_op_impl

private:	// New impl. stubs for subclasses to override
    virtual REPtrT unary_set_op_impl(RECPtrT region, UnarySetOperation op) const = 0;
    virtual void inplace_unary_set_op_impl(REPtrT region_and_out, UnarySetOperation op) const; // Default impl: non-inplace + std::move at end

    virtual REPtrT binary_set_op_impl(RECPtrT left, RECPtrT right, NArySetOperation op) const = 0;
    virtual void inplace_binary_set_op_impl(REPtrT left_and_out, RECPtrT right, NArySetOperation op) const; // Default impl: non-inplace + std::move at end

	// Guaranteed to receive two or more operands
    virtual REPtrT nary_set_op_impl(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const; // Default impl: priority queue with binary setops

	// Guarangeeed to receive two or more operands (including first_and_out)
    virtual void inplace_nary_set_op_impl(REPtrT first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const; // Default impl: non-inplace + std::move at end

protected: // Helper functions
	struct RECompareReverseSizeOrder {
		bool operator() (const RECPtrT &lhs, const RECPtrT &rhs) const { return lhs->get_size_in_bytes() > rhs->get_size_in_bytes(); }
	};

	REPtrT nary_set_op_default_impl(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const; // Default impl: priority queue with binary setops

	// Basic implementations, used as defaults and may be called by subclasses
	template<typename RECompare>
	REPtrT nary_set_op_heapbased(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const; // use priority queue with binary setops
	REPtrT nary_set_op_direct(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const; // use sequential pairwise binary setops
};



// Attempts to apply any of a list of AbstractSetOperations to a given set of operands; the first AbstractSetOperations that
// accepts all operand types is applied.
class PreferenceListSetOperations : public AbstractSetOperations, public std::vector< boost::shared_ptr< AbstractSetOperations > > {
public:
	virtual bool can_handle_region_encoding(RegionEncoding::Type type) const;

private: // AbstractSetOperations impl. stub implementations
	using AbstractSetOperations::REPtr;
	using AbstractSetOperations::RECPtr;
	using AbstractSetOperations::RegionEncodingCPtrCIter;

	virtual REPtr dynamic_unary_set_op_impl(RECPtr region, UnarySetOperation op) const;
	virtual void dynamic_inplace_unary_set_op_impl(REPtr region_and_out, UnarySetOperation op) const;

	virtual REPtr dynamic_binary_set_op_impl(RECPtr left, RECPtr right, NArySetOperation op) const;
	virtual void dynamic_inplace_binary_set_op_impl(REPtr left_and_out, RECPtr right, NArySetOperation op) const;

	virtual REPtr dynamic_nary_set_op_impl(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const;
	virtual void dynamic_inplace_nary_set_op_impl(REPtr first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const;
};

// Chooses between two underlying set operation implementations based on the arity of the requested operation
class ArityThresholdSetOperations : public AbstractSetOperations {
public:
	ArityThresholdSetOperations(boost::shared_ptr< AbstractSetOperations > below, boost::shared_ptr< AbstractSetOperations > at_or_above, size_t arity_thresh) :
		below(below), at_or_above(at_or_above), arity_thresh(arity_thresh)
	{
		assert(arity_thresh >= 2);
	}

	virtual bool can_handle_region_encoding(RegionEncoding::Type type) const {
		return below->can_handle_region_encoding(type) && at_or_above->can_handle_region_encoding(type);
	}

private: // AbstractSetOperations impl. stub implementations
	virtual boost::shared_ptr< RegionEncoding > dynamic_unary_set_op_impl(boost::shared_ptr< const RegionEncoding > region, UnarySetOperation op) const {
		return below->dynamic_unary_set_op(region, op);
	}

	virtual boost::shared_ptr< RegionEncoding > dynamic_binary_set_op_impl(boost::shared_ptr< const RegionEncoding > left, boost::shared_ptr< const RegionEncoding > right, NArySetOperation op) const {
		if (arity_thresh <= 2)
			return at_or_above->dynamic_binary_set_op(left, right, op);
		else
			return below->dynamic_binary_set_op(left, right, op);
	}

	virtual void dynamic_inplace_unary_set_op_impl(boost::shared_ptr< RegionEncoding > region_and_out, UnarySetOperation op) const {
		below->dynamic_inplace_unary_set_op(region_and_out, op);
	}

	virtual void dynamic_inplace_binary_set_op_impl(boost::shared_ptr< RegionEncoding > left_and_out, boost::shared_ptr< const RegionEncoding > right, NArySetOperation op) const {
		if (arity_thresh <= 2)
			at_or_above->dynamic_inplace_binary_set_op(left_and_out, right, op);
		else
			below->dynamic_inplace_binary_set_op(left_and_out, right, op);
	}

	using AbstractSetOperations::RegionEncodingCPtrCIter;
	virtual boost::shared_ptr< RegionEncoding > dynamic_nary_set_op_impl(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const {
		const size_t arity = std::distance(it, end_it);
		if (arity < arity_thresh)
			return below->dynamic_nary_set_op_impl(it, end_it, op);
		else
			return at_or_above->dynamic_nary_set_op_impl(it, end_it, op);
	}

	virtual void dynamic_inplace_nary_set_op_impl(boost::shared_ptr< RegionEncoding > first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const {
		const size_t arity = std::distance(it, end_it);
		if (arity < arity_thresh)
			below->dynamic_inplace_nary_set_op(first_and_out, it, end_it, op);
		else
			at_or_above->dynamic_inplace_nary_set_op(first_and_out, it, end_it, op);
	}

private:
	boost::shared_ptr< AbstractSetOperations > below, at_or_above;
	const size_t arity_thresh;
};



template<typename SetOperationsBasisT>
class GroupedNArySetOperations : public SetOperationsBasisT {
public:
	typedef SetOperationsBasisT SetOperationsBasis;
	typedef typename SetOperationsBasisT::RegionEncodingType RegionEncodingBasis;
	typedef typename SetOperationsBasisT::SetOperationsConfig SetOperationsConfigBasis;

	struct Config {
		Config(int group_size, SetOperationsConfigBasis basis_conf) :
			group_size(group_size),
			basis_conf(basis_conf)
		{}

		int group_size;
		SetOperationsConfigBasis basis_conf;
	};

	typedef RegionEncodingBasis RegionEncodingType;
	typedef Config SetOperationsConfig;

public:
	GroupedNArySetOperations(const Config &conf) :
		SetOperationsBasis(conf.basis_conf),
		group_size(conf.group_size)
	{}
	virtual ~GroupedNArySetOperations() throw() {} // Why is throw() needed? I don't know...

protected:
	typedef typename std::vector< boost::shared_ptr< const RegionEncodingBasis > >::const_iterator RegionEncodingCPtrCIter;
    virtual boost::shared_ptr< RegionEncodingBasis > nary_set_op_impl(RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const;
    virtual void inplace_nary_set_op_impl(boost::shared_ptr< RegionEncodingBasis > first_and_out, RegionEncodingCPtrCIter it, RegionEncodingCPtrCIter end_it, NArySetOperation op) const;

private:
    const int group_size;
};

#include "impl/setops-impl.hpp"

#endif /* SETOPS_HPP_ */
