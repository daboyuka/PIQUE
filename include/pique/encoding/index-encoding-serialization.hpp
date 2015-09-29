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
 * index-encoding-serialization.hpp
 *
 *  Created on: Sep 1, 2015
 *      Author: David A. Boyuka II
 */
#ifndef SRC_ENCODING_INDEX_ENCODING_SERIALIZATION_HPP_
#define SRC_ENCODING_INDEX_ENCODING_SERIALIZATION_HPP_

#include "pique/util/dynamic-pointer-serialization.hpp"

#include "pique/encoding/all-index-encodings.hpp"
#include "pique/encoding/impl/index-encoding-dispatch.hpp"

template<typename Archive> void serialize_encoding(Archive &ar, boost::shared_ptr< IndexEncoding > &index_enc_ptr) {
	IndexEncoding::Type enc_type = Archive::is_saving::value ? index_enc_ptr->get_type() : (IndexEncoding::Type)0; // if loading, value irrelevant
	SerializeDynamicPointer< IndexEncoding, EncTypeDispatch, char >::serialize_pointer(ar, index_enc_ptr, enc_type);
}

template<typename Archive> void serialize_encoding(Archive &ar, boost::shared_ptr< const IndexEncoding > &index_enc_ptr) {
	IndexEncoding::Type enc_type = Archive::is_saving::value ? index_enc_ptr->get_type() : (IndexEncoding::Type)0; // if loading, value irrelevant
	SerializeDynamicPointer< IndexEncoding, EncTypeDispatch, char >::serialize_pointer(ar, index_enc_ptr, enc_type);
}

#endif /* SRC_ENCODING_INDEX_ENCODING_SERIALIZATION_HPP_ */
