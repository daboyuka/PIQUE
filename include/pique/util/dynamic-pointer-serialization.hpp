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
 * dynamic-pointer-serialization.hpp
 *
 *  Created on: Nov 26, 2014
 *      Author: David A. Boyuka II
 */
#ifndef DYNAMIC_POINTER_SERIALIZATION_HPP_
#define DYNAMIC_POINTER_SERIALIZATION_HPP_

#include <boost/utility/enable_if.hpp>
#include <boost/serialization/nvp.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/traits.hpp>
#include <boost/serialization/wrapper.hpp>

template<typename BaseT, typename ValueToTypeDispatchT, typename SerializeValueAsT = char>
class SerializeDynamicPointer :
	public boost::serialization::wrapper_traits<
		boost::serialization::nvp<
			const SerializeDynamicPointer< BaseT, ValueToTypeDispatchT, SerializeValueAsT > > >
{
public:
	using BaseType = BaseT;
	using ValueToTypeDispatchType = ValueToTypeDispatchT;
	using ValueKeyType = typename ValueToTypeDispatchType::ValueType;
	using SerializeValueAsType = SerializeValueAsT;

	SerializeDynamicPointer(boost::shared_ptr< BaseType > &base_ptr, ValueKeyType &base_ptr_type) :
		base_ptr(base_ptr), base_ptr_type(base_ptr_type)
	{}

private:
	template<typename Archive>
	struct SerializeHelper {
		template<typename DerivedClass>
		bool operator()(Archive &ar, boost::shared_ptr< BaseType > &base_ptr, ValueKeyType base_ptr_type) {
			boost::shared_ptr< DerivedClass > deriv_ptr;
			if (Archive::is_loading::value)
				deriv_ptr = boost::make_shared< DerivedClass >();
			else
				deriv_ptr = boost::dynamic_pointer_cast< DerivedClass >(base_ptr);

			ar & (*deriv_ptr);
			return true;
		}

		template<typename DerivedClass, typename Archive2 = Archive, typename boost::enable_if< typename Archive2::is_loading, bool >::type = 0 >
		bool operator()(Archive &ar, boost::shared_ptr< const BaseType > &base_ptr, ValueKeyType base_ptr_type) {
			boost::shared_ptr< DerivedClass > deriv_ptr = boost::make_shared< DerivedClass >();
			ar & (*deriv_ptr);
			base_ptr = deriv_ptr;
			return true;
		}

		template<typename DerivedClass, typename Archive2 = Archive, typename boost::enable_if< typename Archive2::is_saving, bool >::type = 0 >
		bool operator()(Archive &ar, boost::shared_ptr< const BaseType > &base_ptr, ValueKeyType base_ptr_type) {
			boost::shared_ptr< const DerivedClass > deriv_ptr = boost::dynamic_pointer_cast< const DerivedClass >(base_ptr);
			BOOST_ASSERT(deriv_ptr);
			ar & (*deriv_ptr);
			return true;
		}
	};

	friend class boost::serialization::access;
    template<typename Archive> void serialize(Archive &ar, const unsigned int version) {
    	// Save/load the actual pointer type
    	ar & reinterpret_cast< SerializeValueAsType & >(this->base_ptr_type);

    	// Save/load the actual content of the pointer
    	const bool success =
    		ValueToTypeDispatchType::template dispatchMatching< bool >(
    			this->base_ptr_type,
    			SerializeHelper< Archive >(), false,
    			ar, this->base_ptr, this->base_ptr_type);

    	BOOST_ASSERT(success); // Ensure we successfully matched the key value to some type
    }

public:
    template<typename Archive>
    static void serialize_pointer(Archive &ar, boost::shared_ptr< BaseType > &base_ptr, ValueKeyType &base_ptr_type) {
    	// Save/load the actual pointer type
    	ar & reinterpret_cast< SerializeValueAsType & >(base_ptr_type);

    	// Save/load the actual content of the pointer
    	const bool success =
    		ValueToTypeDispatchType::template dispatchMatching< bool >(
    			base_ptr_type,
    			SerializeHelper< Archive >(), false,
    			ar, base_ptr, base_ptr_type);

    	BOOST_ASSERT(success); // Ensure we successfully matched the key value to some type
    }

    template<typename Archive>
    static void serialize_pointer(Archive &ar, boost::shared_ptr< const BaseType > &base_ptr, ValueKeyType &base_ptr_type) {
    	// Save/load the actual pointer type
    	ar & reinterpret_cast< SerializeValueAsType & >(base_ptr_type);

    	// Save/load the actual content of the pointer
    	const bool success =
    		ValueToTypeDispatchType::template dispatchMatching< bool >(
    			base_ptr_type,
    			SerializeHelper< Archive >(), false,
    			ar, base_ptr, base_ptr_type);

    	BOOST_ASSERT(success); // Ensure we successfully matched the key value to some type
    }

private:
	boost::shared_ptr< BaseType > &base_ptr;
	ValueKeyType &base_ptr_type;
};

namespace boost { namespace serialization {
template<typename BaseT, typename ValueToTypeDispatchT, typename SerializeValueAsT>
struct is_wrapper_impl< SerializeDynamicPointer< BaseT, ValueToTypeDispatchT, SerializeValueAsT > > : mpl::true_ {};
}} // namespace

#endif /* DYNAMIC_POINTER_SERIALIZATION_HPP_ */
