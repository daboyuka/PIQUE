// (C) Copyright 2002 Robert Ramey - http://www.rrsd.com .
// (C) Copyright 2014 David A. Boyuka II
// Use, modification and distribution of this file is subject to the Boost Software
// License, Version 1.0. (See http://www.boost.org/LICENSE_1_0.txt)
//
// See http://www.boost.org for updates, documentation, and revision history.
//
// Note: this file is a derivative work of the Boost Serialization library. Its
// purpose is to eliminate spurious filesystem sync() calls in the
// iarchive/oarchive classes.

/*
 * fixed-iarchive-impl.hpp
 *
 *  Created on: Sep 23, 2014
 *      Author: David A. Boyuka II
 */
#ifndef FIXED_IARCHIVE_IMPL_HPP_
#define FIXED_IARCHIVE_IMPL_HPP_

#include <istream>
#include <boost/config.hpp>
#include <boost/integer_traits.hpp>
#include <boost/detail/workaround.hpp>
#include <boost/archive/detail/register_archive.hpp>
#include <boost/archive/detail/common_iarchive.hpp>
#include <boost/serialization/pfto.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/item_version_type.hpp>
#include <boost/serialization/collection_size_type.hpp>


#ifdef BOOST_MSVC
#  pragma warning(push)
#  pragma warning(disable : 4511 4512)
#endif

// note special treatment of shared_ptr. This type needs a special
// structure associated with every archive.  We created a "mix-in"
// class to provide this functionality.  Since shared_ptr holds a
// special esteem in the boost library - we included it here by default.
#include <boost/archive/shared_ptr_helper.hpp>

namespace boost {
namespace archive {

template<class Archive, class Elem, class Tr>
class simple_binary_iarchive_impl :
	public detail::common_iarchive<Archive>
{
protected:
    typedef detail::common_iarchive<Archive> detail_common_iarchive;

    simple_binary_iarchive_impl(std::basic_streambuf<Elem, Tr> & bsb, unsigned int flags) :
        detail_common_iarchive(flags),
    	m_sb(bsb)
    {}

	BOOST_ARCHIVE_OR_WARCHIVE_DECL(void) init();

public:
    // BASIC BYTEWISE SERIALIZATION

    // we provide an optimized load for all fundamental types
    // typedef serialization::is_bitwise_serializable<mpl::_1>
    // use_array_optimization;
    struct use_array_optimization {
        template <class T>
        #if defined(BOOST_NO_DEPENDENT_NESTED_DERIVATIONS)
            struct apply {
                typedef BOOST_DEDUCED_TYPENAME boost::serialization::is_bitwise_serializable< T >::type type;
            };
        #else
            struct apply : public boost::serialization::is_bitwise_serializable< T > {};
        #endif
    };

    // the optimized load_array dispatches to load_binary
    template <class ValueType>
    void load_array(serialization::array<ValueType>& a, unsigned int) {
      load_binary(a.address(),a.count()*sizeof(ValueType));
    }

    void load_binary(void *address, std::size_t count);

protected:
    // SERIALIZATION OF BINARY OBJECTS

    // main template for serialization of primitive types
    template<class T>
    void load(T & t) { load_binary(& t, sizeof(T)); }

    /////////////////////////////////////////////////////////
    // fundamental types that need special treatment

    // trap usage of invalid uninitialized boolean
    void load(bool & t) {
        load_binary(& t, sizeof(t));
        int i = t;
        BOOST_ASSERT(0 == i || 1 == i);
        (void)i; // warning suppression for release builds.
    }
    BOOST_ARCHIVE_OR_WARCHIVE_DECL(void) load(std::string &s);
    #ifndef BOOST_NO_STD_WSTRING
    BOOST_ARCHIVE_OR_WARCHIVE_DECL(void) load(std::wstring &ws);
    #endif
    BOOST_ARCHIVE_OR_WARCHIVE_DECL(void) load(char * t);
    BOOST_ARCHIVE_OR_WARCHIVE_DECL(void) load(wchar_t * t);

    // SERIALIZATION OF PRIMITIVES

    // include these to trap a change in binary format which
    // isn't specifically handled
    // upto 32K classes
    BOOST_STATIC_ASSERT(sizeof(class_id_type) == sizeof(int_least16_t));
    BOOST_STATIC_ASSERT(sizeof(class_id_reference_type) == sizeof(int_least16_t));
    // upto 2G objects
    BOOST_STATIC_ASSERT(sizeof(object_id_type) == sizeof(uint_least32_t));
    BOOST_STATIC_ASSERT(sizeof(object_reference_type) == sizeof(uint_least32_t));

    // binary files don't include the optional information
    void load_override(class_id_optional_type & /* t */, int){}

    void load_override(tracking_type & t, int /*version*/){
    	int_least8_t x=0;
    	* this->This() >> x;
    	t = boost::archive::tracking_type(x);
    }
    void load_override(class_id_reference_type & t, int version){
        load_override(static_cast<class_id_type &>(t), version);
    }
    BOOST_ARCHIVE_OR_WARCHIVE_DECL(void) load_override(class_name_type & t, int);

//    void load_override(class_id_type & t, int version){
//    	this->detail_common_iarchive::load_override(t, version);
//    }
//    void load_override(version_type & t, int version){
//        this->detail_common_iarchive::load_override(t, version);
//    }
//    void load_override(boost::serialization::item_version_type & t, int version){
//    	this->detail_common_iarchive::load_override(t, version);
//    }
//    void load_override(serialization::collection_size_type & t, int version){
//    	this->detail_common_iarchive::load_override(t, version);
//    }

    // CATCH-ALL SERIALIZATION (compiler bug workaround)

    // support override of operators
    // fot templates in the absence of partial function
    // template ordering. If we get here pass to base class
    // note extra nonsense to sneak it pass the borland compiers
    template<class T>
    void load_override(T & t, BOOST_PFTO int version){
      this->detail_common_iarchive::load_override(t, static_cast<int>(version));
    }

protected:
    // return a pointer to the most derived class
    Archive * This() { return static_cast<Archive *>(this); }

    std::basic_streambuf<Elem, Tr> & m_sb;

    friend class detail::interface_iarchive<Archive>;
    friend class load_access;
};

// do not derive from this class.  If you want to extend this functionality
// via inhertance, derived from binary_iarchive_impl instead.  This will
// preserve correct static polymorphism.
class simple_binary_iarchive :
    public simple_binary_iarchive_impl< boost::archive::simple_binary_iarchive, std::istream::char_type, std::istream::traits_type >,
    public detail::shared_ptr_helper
{
public:
	using ParentType = simple_binary_iarchive_impl< boost::archive::simple_binary_iarchive, std::istream::char_type, std::istream::traits_type >;
	simple_binary_iarchive(std::istream & is, unsigned int flags = 0) :
		ParentType(*is.rdbuf(), flags)
    {}
	simple_binary_iarchive(std::streambuf & bsb, unsigned int flags = 0) :
		ParentType(bsb, flags)
    {}
};



} // namespace archive
} // namespace boost

// required by export
BOOST_SERIALIZATION_REGISTER_ARCHIVE(boost::archive::simple_binary_iarchive)
BOOST_SERIALIZATION_USE_ARRAY_OPTIMIZATION(boost::archive::simple_binary_iarchive)

// Impls

template<class Archive, class Elem, class Tr>
BOOST_ARCHIVE_OR_WARCHIVE_DECL(void)
boost::archive::simple_binary_iarchive_impl<Archive, Elem, Tr>::load_override(class_name_type & t, int) {
    std::string cn;
    cn.reserve(BOOST_SERIALIZATION_MAX_KEY_SIZE);
    load_override(cn, 0);
    if(cn.size() > (BOOST_SERIALIZATION_MAX_KEY_SIZE - 1))
        boost::serialization::throw_exception(
            archive_exception(archive_exception::invalid_class_name)
        );
    std::memcpy(t, cn.data(), cn.size());
    // borland tweak
    t.t[cn.size()] = '\0';
}

template<class Archive, class Elem, class Tr>
BOOST_ARCHIVE_OR_WARCHIVE_DECL(void)
boost::archive::simple_binary_iarchive_impl<Archive, Elem, Tr>::load(wchar_t * ws)
{
    std::size_t l; // number of wchar_t !!!
    this->This()->load(l);
    load_binary(ws, l * sizeof(wchar_t) / sizeof(char));
    ws[l] = L'\0';
}

template<class Archive, class Elem, class Tr>
BOOST_ARCHIVE_OR_WARCHIVE_DECL(void)
boost::archive::simple_binary_iarchive_impl<Archive, Elem, Tr>::load(std::string & s)
{
    std::size_t l;
    this->This()->load(l);
    // borland de-allocator fixup
    #if BOOST_WORKAROUND(_RWSTD_VER, BOOST_TESTED_AT(20101))
    if(NULL != s.data())
    #endif
        s.resize(l);
    // note breaking a rule here - could be a problem on some platform
    if(0 < l)
        load_binary(&(*s.begin()), l);
}

#ifndef BOOST_NO_CWCHAR
template<class Archive, class Elem, class Tr>
BOOST_ARCHIVE_OR_WARCHIVE_DECL(void)
boost::archive::simple_binary_iarchive_impl<Archive, Elem, Tr>::load(char * s)
{
    std::size_t l;
    this->This()->load(l);
    load_binary(s, l);
    s[l] = '\0';
}
#endif

#ifndef BOOST_NO_STD_WSTRING
template<class Archive, class Elem, class Tr>
BOOST_ARCHIVE_OR_WARCHIVE_DECL(void)
boost::archive::simple_binary_iarchive_impl<Archive, Elem, Tr>::load(std::wstring & ws)
{
    std::size_t l;
    this->This()->load(l);
    // borland de-allocator fixup
    #if BOOST_WORKAROUND(_RWSTD_VER, BOOST_TESTED_AT(20101))
    if(NULL != ws.data())
    #endif
        ws.resize(l);
    // note breaking a rule here - is could be a problem on some platform
    load_binary(const_cast<wchar_t *>(ws.data()), l * sizeof(wchar_t) / sizeof(char));
}
#endif

template<class Archive, class Elem, class Tr>
inline void
boost::archive::simple_binary_iarchive_impl<Archive, Elem, Tr>::load_binary(void *address, std::size_t count){
    // note: an optimizer should eliminate the following for char files
    BOOST_ASSERT(
        static_cast<std::streamsize>(count / sizeof(Elem))
        <= boost::integer_traits<std::streamsize>::const_max
    );
    std::streamsize s = static_cast<std::streamsize>(count / sizeof(Elem));
    std::streamsize scount = m_sb.sgetn(
        static_cast<Elem *>(address),
        s
    );
    if(scount != s)
        boost::serialization::throw_exception(
            archive_exception(archive_exception::input_stream_error)
        );
    // note: an optimizer should eliminate the following for char files
    BOOST_ASSERT(count % sizeof(Elem) <= boost::integer_traits<std::streamsize>::const_max);
    s = static_cast<std::streamsize>(count % sizeof(Elem));
    if(0 < s){
//        if(is.fail())
//            boost::serialization::throw_exception(
//                archive_exception(archive_exception::stream_error)
//        );
        Elem t;
        scount = m_sb.sgetn(& t, 1);
        if(scount != 1)
            boost::serialization::throw_exception(
                archive_exception(archive_exception::input_stream_error)
            );
        std::memcpy(static_cast<char*>(address) + (count - s), &t, s);
    }
}

#endif /* FIXED_IARCHIVE_IMPL_HPP_ */
