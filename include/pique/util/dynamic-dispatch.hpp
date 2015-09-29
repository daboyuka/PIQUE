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
 * dynamic-dispatch.hpp
 *
 *  Created on: Aug 12, 2014
 *      Author: David A. Boyuka II
 */
#ifndef DYNAMIC_DISPATCH_HPP_
#define DYNAMIC_DISPATCH_HPP_

#include <boost/type_traits.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>
#include <boost/none.hpp>
#include <boost/make_shared.hpp>

/*
 * Example Usage:
 *
 * enum struct E { A, B, C };
 * struct EDispatchFns {
 *   template<typename DispatchType>
 *   void operator()(E val) {
 *      // Do something with DispatchType and/or val
 *   }
 * };
 * struct TypeDispatchFns {
 *   template<typename DispatchType>
 *   void operator()() {
 *   	// Do something with DispatchType
 *   }
 * };
 *
 * auto dispatcher = make_value_to_type_dispatch
 * 		<	DispatchTypeA,	DispatchTypeB,	DispatchTypeC	>
 * 		(  E::A,			E::B, 			E::C			);
 * dispatcher.dispatchMatching<>(EDispatchFns(), E::B); // calls dispatch.operator()<DispatchTypeB>(E::B), where dispatch = EDispatchFns()
 * dispatcher.dispatchAll<>(EDispatchFns());            // calls dispatch.operator()<DispatchTypeA>(E::A), then similarly for B, then C, where dispatch = EDispatchFns()
 *
 * TypeDispatch< DispatchTypeA, DispatchTypeB, DispatchTypeC > dispatcher2;
 * dispatcher2.dispatchAll<>(TypeDispatchFns()); // calls dispatch.operator()<DispatchTypeA>(), then similarly for B, then C, where dispatch = TypeDispatchFns()
 *
 */

template<typename ValueType, ValueType Value, typename ToType>
struct ValueToType {
	static constexpr ValueType value = Value;
	typedef ToType type;
};

template<typename ValueToTypeEntry, typename... OtherValueToTypeEntries>
class ValueToTypeDispatchBase : private ValueToTypeDispatchBase<OtherValueToTypeEntries...> {
public:
	using Parent = ValueToTypeDispatchBase<OtherValueToTypeEntries...>;
	using ValueType = typename boost::remove_const< decltype(ValueToTypeEntry::value) >::type;
	static constexpr ValueType FirstDispatchValue = ValueToTypeEntry::value;
	using FirstDispatchType = typename ValueToTypeEntry::type;

	template<typename ReturnType, typename PredicateFunctor, typename DispatchFunctor, typename... ArgTypes>
	static ReturnType dispatchPredicate(PredicateFunctor predicate, DispatchFunctor dispatch, ReturnType defaultRetValue, ArgTypes&&... args) {
		if (predicate.template operator()<FirstDispatchType>(FirstDispatchValue)) {
			return dispatch.template operator()<FirstDispatchType>(FirstDispatchValue, std::forward<ArgTypes>(args)...);
		} else
			return Parent::template dispatchPredicate< ReturnType >(predicate, dispatch, std::move(defaultRetValue), std::forward<ArgTypes>(args)...);
	}
};

template<typename ValueToTypeEntry>
class ValueToTypeDispatchBase<ValueToTypeEntry> {
public:
	using ValueType = typename boost::remove_const< decltype(ValueToTypeEntry::value) >::type;
	static constexpr ValueType LastDispatchValue = ValueToTypeEntry::value;
	using LastDispatchType = typename ValueToTypeEntry::type;

	template<typename ReturnType, typename PredicateFunctor, typename DispatchFunctor, typename... ArgTypes>
	static ReturnType dispatchPredicate(PredicateFunctor predicate, DispatchFunctor dispatch, ReturnType defaultRetValue, ArgTypes&&... args) {
		if (predicate.template operator()<LastDispatchType>(LastDispatchValue)) {
			return dispatch.template operator()<LastDispatchType>(LastDispatchValue, std::forward<ArgTypes>(args)...);
		} else
			return defaultRetValue;
	}
};

template<typename ValueToTypeEntry, typename... OtherValueToTypeEntries>
class ValueToTypeDispatch : public ValueToTypeDispatchBase< ValueToTypeEntry, OtherValueToTypeEntries... > {
public:
	using Parent = ValueToTypeDispatchBase< ValueToTypeEntry, OtherValueToTypeEntries... >;
	using ValueType = typename boost::remove_const< decltype(ValueToTypeEntry::value) >::type;
	static constexpr ValueType FirstDispatchValue = ValueToTypeEntry::value;
	using FirstDispatchType = typename ValueToTypeEntry::type;

private:
	struct MatchValuePredicate {
		MatchValuePredicate(ValueType matchingVal) : matchingVal(matchingVal) {}

		template<typename DispatchType>
		bool operator()(ValueType val) { return val == matchingVal; }
	private:
		ValueType matchingVal;
	};

	template<typename ReturnType, typename DispatchFunctor, typename... ArgTypes>
	struct MatchValueDispatch {
		MatchValueDispatch(DispatchFunctor dispatch) : dispatch(std::move(dispatch)) {}

		template<typename DispatchType>
		ReturnType operator()(ValueType val, ArgTypes&&... args) {
			return dispatch.template operator()<DispatchType>(std::forward<ArgTypes>(args)...);
		}
	private:
		DispatchFunctor dispatch;
	};

	template<typename DispatchFunctor>
	struct AlwaysFalseDispatcherPredicate {
		AlwaysFalseDispatcherPredicate(DispatchFunctor dispatch) : dispatch(std::move(dispatch)) {}

		template<typename DispatchType>
		bool operator()(ValueType val) { dispatch.template operator()<DispatchType>(val); return false; }
	private:
		DispatchFunctor dispatch;
	};

	struct NoOpDispatch {
		template<typename DispatchType>
		void operator()(ValueType val) {}
	};

public:
	template<typename ReturnType, typename DispatchFunctor, typename... ArgTypes>
	static ReturnType dispatchMatching(ValueType matchingVal, DispatchFunctor dispatch, ReturnType defaultRetValue, ArgTypes&&... args) {
		return Parent::template dispatchPredicate< ReturnType >(
				MatchValuePredicate(matchingVal), // returns true when matchingVal is found
				MatchValueDispatch<ReturnType, DispatchFunctor, ArgTypes...>(dispatch), // strips matchingVal from the dispatch arg list
				std::move(defaultRetValue),
				std::forward<ArgTypes>(args)...);
	}

	template<typename DispatchFunctor>
	static void dispatchAll(DispatchFunctor dispatch) {
		return Parent::template dispatchPredicate<void>(
				AlwaysFalseDispatcherPredicate<DispatchFunctor>(dispatch), // always returns false, but calls dispatch each time it is called
				NoOpDispatch(), // dispatch should never be called, but a no-op is supplied to make C++ happy
				void());
	}
};

template<typename Type, typename Ignore>
struct IgnoreSecondType { typedef Type type; };

template<typename ValueType>
struct MakeValueToTypeDispatch {
	template<ValueType... Values>
	struct WithValues {
		template<typename... Types>
		struct WithTypes {
			typedef ValueToTypeDispatch< ValueToType< ValueType, Values, Types >... > type;
		};
	};
};

template<typename... TypeDispatches>
class TypeDispatch : private ValueToTypeDispatch< ValueToType<int, 0, TypeDispatches>... > {
private:
	template<typename Ret, typename Fn>
	class IgnoreIntArg {
	public:
		IgnoreIntArg(Fn fn) : fn(fn) {}
		template<typename DispatchType> Ret operator()(int /*ignored*/) { return fn.template operator()<DispatchType>(); }
	private:
		Fn fn;
	};
public:
	using Parent = ValueToTypeDispatch< ValueToType<int, 0, TypeDispatches>... >;

	template<typename ReturnType, typename PredicateFunctor, typename DispatchFunctor, typename... ArgTypes>
	static ReturnType dispatchPredicate(PredicateFunctor predicate, DispatchFunctor dispatch, ReturnType defaultRetValue, ArgTypes&&... args) {
		return Parent::template dispatchPredicate< ReturnType >(IgnoreIntArg<bool, PredicateFunctor>(predicate), IgnoreIntArg<ReturnType, DispatchFunctor>(dispatch), std::move(defaultRetValue), std::forward<ArgTypes>(args)...);
	}

	template<typename DispatchFunctor>
	static void dispatchAll(DispatchFunctor dispatch) {
		Parent::template dispatchAll<>(IgnoreIntArg<void, DispatchFunctor>(dispatch));
	}
};

template<typename ValueType, ValueType FirstValue, ValueType... Values>
class ValueDispatch : private ValueDispatch<ValueType, Values...> {
public:
	using Parent = ValueDispatch<ValueType, Values...>;

	template<typename ReturnType, typename DispatchFunctor, typename... ArgTypes>
	static ReturnType dispatchMatching(ValueType matchingVal, DispatchFunctor dispatch, ReturnType defaultReturnVal, ArgTypes&&... args) {
		if (matchingVal == FirstValue)
			return dispatch.template operator()<FirstValue>(std::forward<ArgTypes>(args)...);
		else
			return Parent::template dispatchMatching< ReturnType >(matchingVal, dispatch, std::move(defaultReturnVal), std::forward<ArgTypes>(args)...);
	}
};

template<typename ValueType, ValueType LastValue>
class ValueDispatch<ValueType, LastValue> {
public:
	template<typename ReturnType, typename DispatchFunctor, typename... ArgTypes>
	static ReturnType dispatchMatching(ValueType matchingVal, DispatchFunctor dispatch, ReturnType defaultReturnVal, ArgTypes&&... args) {
		if (matchingVal == LastValue)
			return dispatch.template operator()<LastValue>(std::forward<ArgTypes>(args)...);
		else
			return defaultReturnVal;
	}
};

namespace boost {
	template<typename ReturnBaseType>
	struct make_shared_dispatch {
		template<typename DerivedType, typename... ArgTypes>
		boost::shared_ptr< ReturnBaseType > operator()(ArgTypes&&... args) {
			return boost::make_shared< DerivedType >(std::forward<ArgTypes>(args)...);
		}
	};
};

#endif /* DYNAMIC_DISPATCH_HPP_ */
