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
 * region-convert.hpp
 *
 *  Created on: Jun 17, 2014
 *      Author: David A. Boyuka II
 */
#ifndef REGION_CONVERT_HPP_
#define REGION_CONVERT_HPP_

#include <boost/smart_ptr.hpp>

#include "pique/region/region-encoding.hpp"
#include "pique/setops/setops.hpp"

class AbstractRegionEncodingConverter {
public:
	using InRegionEncodingType = RegionEncoding;
	using OutRegionEncodingType = RegionEncoding;

	virtual ~AbstractRegionEncodingConverter() {}

	virtual bool can_convert_region_encoding(RegionEncoding::Type intype, RegionEncoding::Type outtype) const = 0;

	virtual boost::shared_ptr< RegionEncoding > convert(boost::shared_ptr< const RegionEncoding > in, RegionEncoding::Type outtype) const = 0;
	virtual boost::shared_ptr< RegionEncoding > inplace_convert(boost::shared_ptr< const RegionEncoding > in_right, boost::shared_ptr< RegionEncoding > out_combine_left, NArySetOperation combine_op) const = 0;
};

template<typename OutRegionEncodingT>
class FixedOutputRegionEncodingConverter : public AbstractRegionEncodingConverter {
public:
	using InRegionEncodingType = RegionEncoding;
	using OutRegionEncodingType = OutRegionEncodingT;

	virtual ~FixedOutputRegionEncodingConverter() {}

	virtual RegionEncoding::Type get_conversion_output_type() const { return OutRegionEncodingT::TYPE; };

	virtual bool can_convert_region_encoding(RegionEncoding::Type intype, RegionEncoding::Type outtype) const {
		return outtype == get_conversion_output_type();
	}

	virtual boost::shared_ptr< RegionEncoding > convert(boost::shared_ptr< const RegionEncoding > in, RegionEncoding::Type outtype) const {
		if (!this->can_convert_region_encoding(in->get_type(), outtype))
			return nullptr;
		return this->convert(in);
	}

	virtual boost::shared_ptr< RegionEncoding > inplace_convert(boost::shared_ptr< const RegionEncoding > in_right, boost::shared_ptr< RegionEncoding > out_combine_left, NArySetOperation combine_op) const {
		if (!this->can_convert_region_encoding(in_right->get_type(), out_combine_left->get_type()))
			return nullptr;
		this->inplace_convert(in_right, boost::static_pointer_cast< OutRegionEncodingT >(out_combine_left), combine_op);
		return out_combine_left;
	}

	virtual boost::shared_ptr< OutRegionEncodingT > convert(boost::shared_ptr< const RegionEncoding > in) const = 0;
	virtual void inplace_convert(boost::shared_ptr< const RegionEncoding > in_right, boost::shared_ptr< OutRegionEncodingT > out_combine_left, NArySetOperation combine_op) const = 0;
};

template<typename InRegionEncodingT, typename OutRegionEncodingT>
class RegionEncodingConverter : public FixedOutputRegionEncodingConverter< OutRegionEncodingT > {
public:
	using InRegionEncodingType = InRegionEncodingT;
	using OutRegionEncodingType = OutRegionEncodingT;
	using ParentType = FixedOutputRegionEncodingConverter< OutRegionEncodingT >;

	virtual ~RegionEncodingConverter() {}

	virtual RegionEncoding::Type get_conversion_input_type() const { return InRegionEncodingT::TYPE; };

	virtual bool can_convert_region_encoding(RegionEncoding::Type intype, RegionEncoding::Type outtype) const {
		return intype == get_conversion_input_type() && this->ParentType::can_convert_region_encoding(intype, outtype);
	}

	virtual boost::shared_ptr< OutRegionEncodingT > convert(boost::shared_ptr< const RegionEncoding > in) const {
		return this->convert(boost::static_pointer_cast< const InRegionEncodingT >(in));
	}

	virtual void inplace_convert(boost::shared_ptr< const RegionEncoding > in_right, boost::shared_ptr< OutRegionEncodingT > out_combine_left, NArySetOperation combine_op) const {
		this->inplace_convert(boost::static_pointer_cast< const InRegionEncodingT >(in_right), out_combine_left, combine_op);
	}

	virtual boost::shared_ptr< OutRegionEncodingT > convert(boost::shared_ptr< const InRegionEncodingT > in) const = 0;
	virtual void inplace_convert(boost::shared_ptr< const InRegionEncodingT > in_right, boost::shared_ptr< OutRegionEncodingT > out_combine_left, NArySetOperation combine_op) const = 0;
};

// Abstract adapter base class, adds inplace conversion capability to any REConverter implementation by converting to a temporary RE,
// then applying a given inplace binary set operation algorithm to finish the process
template<typename InRegionEncodingT, typename OutRegionEncodingT>
class RegionEncodingOutOfPlaceConverter : public RegionEncodingConverter<InRegionEncodingT, OutRegionEncodingT> {
public:
	RegionEncodingOutOfPlaceConverter(std::unique_ptr< SetOperations< OutRegionEncodingT > > setops) :
		setops(setops)
	{
		assert(setops->can_handle_region_encoding(OutRegionEncodingT::TYPE)); // Just double-checking
	}
	virtual ~RegionEncodingOutOfPlaceConverter() {}

	//virtual boost::shared_ptr< OutRegionEncodingT > convert(boost::shared_ptr< const InRegionEncodingT > in) const = 0; // Still pure virtual; implement in subclass

	virtual void inplace_convert(boost::shared_ptr< const InRegionEncodingT > in_right, boost::shared_ptr< OutRegionEncodingT > out_combine_left, NArySetOperation combine_op) const {
		boost::shared_ptr< OutRegionEncodingT > tmp = this->convert(in_right); // Convert into a temp. RE
		setops->inplace_binary_set_op(out_combine_left, tmp, combine_op); // Inplace combine the temp. RE into the output RE
	}

private:
	std::unique_ptr< SetOperations< OutRegionEncodingT > > setops;
};

// Attempts to apply any of a list of AbstractSetOperations to a given set of operands; the first AbstractSetOperations that
// accepts all operand types is applied.
template<typename BaseConverterT>
class PreferenceListRegionEncodingConverterBase : public BaseConverterT, public std::vector< boost::shared_ptr< BaseConverterT > > {
public:
	using BaseConverterType = BaseConverterT;
	using InRegionEncodingType = typename BaseConverterT::InRegionEncodingType;
	using OutRegionEncodingType = typename BaseConverterT::OutRegionEncodingType;
	virtual ~PreferenceListRegionEncodingConverterBase() {}

private:
	boost::shared_ptr< BaseConverterType > get_preferred_converter(RegionEncoding::Type intype, RegionEncoding::Type outtype) const {
		for (auto it = this->begin(); it != this->end(); it++)
			if ((*it)->can_convert_region_encoding(intype, outtype))
				return (*it);
		return nullptr;
	}

public:
	virtual bool can_convert_region_encoding(RegionEncoding::Type intype, RegionEncoding::Type outtype) const {
		return this->get_preferred_converter(intype, outtype) != nullptr;
	}

	boost::shared_ptr< OutRegionEncodingType > convert_base(boost::shared_ptr< const RegionEncoding > in, RegionEncoding::Type outtype) const {
		boost::shared_ptr< BaseConverterType > converter = this->get_preferred_converter(in->get_type(), outtype);
		if (converter) {
			return boost::static_pointer_cast< OutRegionEncodingType >(
				converter->convert(boost::static_pointer_cast< const InRegionEncodingType >(in), outtype));
		} else {
			abort();
			return nullptr;
		}
	}

	boost::shared_ptr< OutRegionEncodingType > inplace_convert_base(boost::shared_ptr< const RegionEncoding > in_right, boost::shared_ptr< RegionEncoding > out_combine_left, NArySetOperation combine_op) const {
		boost::shared_ptr< BaseConverterType > converter = this->get_preferred_converter(in_right->get_type(), out_combine_left->get_type());
		if (converter) {
			boost::shared_ptr< OutRegionEncodingType > out_combine_left_typed =
				boost::static_pointer_cast< OutRegionEncodingType >(out_combine_left);
			converter->inplace_convert(
				boost::static_pointer_cast< const InRegionEncodingType >(in_right),
				out_combine_left_typed,
				combine_op);
			return out_combine_left_typed;
		} else {
			abort();
			return nullptr;
		}
	}
};

class PreferenceListRegionEncodingConverter : public PreferenceListRegionEncodingConverterBase< AbstractRegionEncodingConverter > {
public:
	using ParentType = PreferenceListRegionEncodingConverterBase< AbstractRegionEncodingConverter >;
	using BaseConverterType = typename ParentType::BaseConverterType;;
	using InRegionEncodingType = typename ParentType::InRegionEncodingType;
	using OutRegionEncodingType = typename ParentType::OutRegionEncodingType;

	virtual ~PreferenceListRegionEncodingConverter() {}

	virtual boost::shared_ptr< RegionEncoding > convert(boost::shared_ptr< RegionEncoding > in, RegionEncoding::Type outtype) const {
		return this->convert_base(in, outtype);
	}
	virtual boost::shared_ptr< RegionEncoding > inplace_convert(boost::shared_ptr< const RegionEncoding > in_right, boost::shared_ptr< RegionEncoding > out_combine_left, NArySetOperation combine_op) const {
		return this->inplace_convert_base(in_right, out_combine_left, combine_op);
	}
};

template<typename OutRegionEncodingT>
class FixedOutputPreferenceListRegionEncodingConverter : public PreferenceListRegionEncodingConverterBase< FixedOutputRegionEncodingConverter< OutRegionEncodingT > > {
public:
	using ParentType = PreferenceListRegionEncodingConverterBase< FixedOutputRegionEncodingConverter< OutRegionEncodingT > >;
	using BaseConverterType = typename ParentType::BaseConverterType;;
	using InRegionEncodingType = typename ParentType::InRegionEncodingType;
	using OutRegionEncodingType = typename ParentType::OutRegionEncodingType;

	virtual ~FixedOutputPreferenceListRegionEncodingConverter() {}

	virtual boost::shared_ptr< OutRegionEncodingT > convert(boost::shared_ptr< const RegionEncoding > in) const {
		return this->convert_base(in, this->get_conversion_output_type());
	}
	virtual void inplace_convert(boost::shared_ptr< const RegionEncoding > in_right, boost::shared_ptr< OutRegionEncodingT > out_combine_left, NArySetOperation combine_op) const {
		this->inplace_convert_base(in_right, out_combine_left, combine_op);
	}
};

#endif /* REGION_CONVERT_HPP_ */
