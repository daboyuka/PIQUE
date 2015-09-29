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
 * fixed-oarchive-impl.hpp
 *
 *  Created on: Sep 23, 2014
 *      Author: David A. Boyuka II
 */
#ifndef FIXED_OARCHIVE_IMPL_HPP_
#define FIXED_OARCHIVE_IMPL_HPP_

#include <cstdlib>
#include <cstring>
#include <string>
#include <ostream>

#include <boost/assert.hpp>
#include <boost/config.hpp>
#include <boost/integer.hpp>
#include <boost/integer_traits.hpp>
#include <boost/detail/workaround.hpp>
#include <boost/archive/detail/common_oarchive.hpp>
#include <boost/archive/detail/register_archive.hpp>
#include <boost/serialization/pfto.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/item_version_type.hpp>
#include <boost/serialization/collection_size_type.hpp>

#ifdef BOOST_MSVC
#  pragma warning(push)
#  pragma warning(disable : 4511 4512)
#endif

namespace boost {
namespace archive {

template<class Archive, class Elem, class Tr>
class simple_binary_oarchive_impl : public boost::archive::detail::common_oarchive< Archive >
{
protected:
    simple_binary_oarchive_impl(std::basic_streambuf<Elem, Tr> & bsb, unsigned int flags) :
        detail::common_oarchive<Archive>(flags),
        m_sb(bsb)
    {}

    BOOST_ARCHIVE_OR_WARCHIVE_DECL(void) init();

public:
    // BASIC BYTEWISE SERIALIZATION

    // we provide an optimized save for all fundamental types
    // typedef serialization::is_bitwise_serializable<mpl::_1>
    // use_array_optimization;
    // workaround without using mpl lambdas
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

    // the optimized save_array dispatches to save_binary
    template <class ValueType>
    void save_array(boost::serialization::array< ValueType > const& a, unsigned int) {
    	save_binary(a.address(), a.count() * sizeof(ValueType));
    }

    void save_binary(const void *address, std::size_t count);

protected:
    // SERIALIZATION OF BINARY OBJECTS

    template<class T>
    void save(const T & t) { save_binary(& t, sizeof(T)); }

    /////////////////////////////////////////////////////////
    // fundamental types that need special treatment

    // trap usage of invalid uninitialized boolean which would
    // otherwise crash on load.
    void save(const bool t){
        BOOST_ASSERT(0 == static_cast<int>(t) || 1 == static_cast<int>(t));
        save_binary(& t, sizeof(t));
    }

    BOOST_ARCHIVE_OR_WARCHIVE_DECL(void) save(const std::string &s);
    #ifndef BOOST_NO_STD_WSTRING
    BOOST_ARCHIVE_OR_WARCHIVE_DECL(void) save(const std::wstring &ws);
    #endif
    BOOST_ARCHIVE_OR_WARCHIVE_DECL(void) save(const char * t);
    BOOST_ARCHIVE_OR_WARCHIVE_DECL(void) save(const wchar_t * t);

    // SERIALIZATION OF PRIMITIVES

    // any datatype not specifed below will be handled by base class
    typedef detail::common_oarchive<Archive> detail_common_oarchive;
    template<class T>
    void save_override(const T & t, BOOST_PFTO int version){
      this->detail_common_oarchive::save_override(t, static_cast<int>(version));
    }

    // include these to trap a change in binary format which
    // isn't specifically handled
    BOOST_STATIC_ASSERT(sizeof(tracking_type) == sizeof(bool));
    // upto 32K classes
    BOOST_STATIC_ASSERT(sizeof(class_id_type) == sizeof(int_least16_t));
    BOOST_STATIC_ASSERT(sizeof(class_id_reference_type) == sizeof(int_least16_t));
    // upto 2G objects
    BOOST_STATIC_ASSERT(sizeof(object_id_type) == sizeof(uint_least32_t));
    BOOST_STATIC_ASSERT(sizeof(object_reference_type) == sizeof(uint_least32_t));

    // binary files don't include the optional information
    void save_override(const class_id_optional_type & /* t */, int) {}

    // explicitly convert to char * to avoid compile ambiguities
    void save_override(const class_name_type & t, int) {
        const std::string s(t);
        *this->This() << s;
    }

protected:
    // return a pointer to the most derived class
    Archive * This() { return static_cast<Archive *>(this); }

    std::basic_streambuf<Elem, Tr> & m_sb;

    friend class detail::interface_oarchive< Archive >;
    friend class save_access;
};

// do not derive from this class.  If you want to extend this functionality
// via inhertance, derived from binary_oarchive_impl instead.  This will
// preserve correct static polymorphism.
class simple_binary_oarchive :
		public simple_binary_oarchive_impl< simple_binary_oarchive, std::ostream::char_type, std::ostream::traits_type >
{
public:
	using ParentType = simple_binary_oarchive_impl< simple_binary_oarchive, std::ostream::char_type, std::ostream::traits_type >;
	simple_binary_oarchive(std::ostream & os, unsigned int flags = 0) :
		ParentType(*os.rdbuf(), flags)
    {}
	simple_binary_oarchive(std::streambuf & bsb, unsigned int flags = 0) :
		ParentType(bsb, flags)
    {}
};

typedef simple_binary_oarchive naked_simple_binary_oarchive;

} // namespace archive
} // namespace boost

// required by export
BOOST_SERIALIZATION_REGISTER_ARCHIVE(boost::archive::simple_binary_oarchive)
BOOST_SERIALIZATION_USE_ARRAY_OPTIMIZATION(boost::archive::simple_binary_oarchive)

// Impls

template<class Archive, class Elem, class Tr>
BOOST_ARCHIVE_OR_WARCHIVE_DECL(void)
boost::archive::simple_binary_oarchive_impl<Archive, Elem, Tr>::init() {}

template<class Archive, class Elem, class Tr>
BOOST_ARCHIVE_OR_WARCHIVE_DECL(void)
boost::archive::simple_binary_oarchive_impl<Archive, Elem, Tr>::save(const char * s) {
    std::size_t l = std::strlen(s);
    this->This()->save(l);
    save_binary(s, l);
}

template<class Archive, class Elem, class Tr>
BOOST_ARCHIVE_OR_WARCHIVE_DECL(void)
boost::archive::simple_binary_oarchive_impl<Archive, Elem, Tr>::save(const std::string &s) {
    std::size_t l = static_cast<std::size_t>(s.size());
    this->This()->save(l);
    save_binary(s.data(), l);
}

#ifndef BOOST_NO_CWCHAR
template<class Archive, class Elem, class Tr>
BOOST_ARCHIVE_OR_WARCHIVE_DECL(void)
boost::archive::simple_binary_oarchive_impl<Archive, Elem, Tr>::save(const wchar_t * ws) {
    std::size_t l = std::wcslen(ws);
    this->This()->save(l);
    save_binary(ws, l * sizeof(wchar_t) / sizeof(char));
}
#endif

#ifndef BOOST_NO_STD_WSTRING
template<class Archive, class Elem, class Tr>
BOOST_ARCHIVE_OR_WARCHIVE_DECL(void)
boost::archive::simple_binary_oarchive_impl<Archive, Elem, Tr>::save(const std::wstring &ws) {
    std::size_t l = ws.size();
    this->This()->save(l);
    save_binary(ws.data(), l * sizeof(wchar_t) / sizeof(char));
}
#endif

template<class Archive, class Elem, class Tr>
inline void
boost::archive::simple_binary_oarchive_impl<Archive, Elem, Tr>::save_binary(const void *address, std::size_t count) {
    //BOOST_ASSERT(
    //    static_cast<std::size_t>((std::numeric_limits<std::streamsize>::max)()) >= count
    //);
    // note: if the following assertions fail
    // a likely cause is that the output stream is set to "text"
    // mode where by cr characters recieve special treatment.
    // be sure that the output stream is opened with ios::binary
    //if (os.fail())
    //    boost::serialization::throw_exception(
    //        archive_exception(archive_exception::output_stream_error)
    //    );
    // figure number of elements to output - round up
    count = (count + sizeof(Elem) - 1) / sizeof(Elem);
    BOOST_ASSERT(count <= std::size_t(boost::integer_traits<std::streamsize>::const_max));
    std::streamsize scount = m_sb.sputn(
        static_cast<const Elem *>(address),
        static_cast<std::streamsize>(count)
    );
    if (count != static_cast<std::size_t>(scount))
        boost::serialization::throw_exception(archive_exception(archive_exception::output_stream_error));
}

#endif /* FIXED_OARCHIVE_IMPL_HPP_ */
