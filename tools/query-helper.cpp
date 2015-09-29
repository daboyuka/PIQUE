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
 * query-helper.cpp
 *
 *  Created on: May 21, 2014
 *      Author: David A. Boyuka II
 */

#include <string>

#include <pique/setops/setops.hpp>
#include <pique/setops/ii/ii-setops.hpp>
#include <pique/setops/cii/cii-setops.hpp>
#include <pique/setops/cblq/cblq-setops.hpp>
#include <pique/setops/wah/wah-setops.hpp>
#include <pique/setops/bitmap/bitmap-setops.hpp>

#include <pique/convert/region-convert.hpp>
#include <pique/convert/cblq/cblq-to-bitmap-convert.hpp>

#include <pique/query/query-engine.hpp>
#include <pique/query/basic-query-engine.hpp>
#include <pique/query/bitmap-query-engine.hpp>

#include "query-helper.hpp"

static constexpr size_t NARY_ARITY_THRESH = 8;
static constexpr size_t NARYFAST_ARITY_THRESH = 4;

template<typename CBLQSetOpsT>
static boost::shared_ptr< AbstractSetOperations > make_arity_thresh(size_t thresh, CBLQSetOperationsConfig conf) {
	return boost::make_shared< ArityThresholdSetOperations >(
			boost::make_shared< CBLQSetOperationsFast< CBLQSetOpsT::NDIM > >(conf),
			boost::make_shared< CBLQSetOpsT >(conf),
			thresh
	);
}

static boost::shared_ptr< BasicQueryEngine > configure_basic_query_engine(std::string setops_mode, QueryEngineOptions options, bool verbose) {
	boost::shared_ptr< PreferenceListSetOperations > preflist_setops = boost::make_shared< PreferenceListSetOperations >();

	// Only one set operations code for II, CII (N-ary seems universally better, but is buggy) and WAH
	preflist_setops->push_back(boost::make_shared< IISetOperations >(IISetOperationsConfig()));
	preflist_setops->push_back(boost::make_shared< CIISetOperations >(CIISetOperationsConfig(false)));
	preflist_setops->push_back(boost::make_shared< WAHSetOperations >(WAHSetOperationsConfig()));

	if (setops_mode == "nary3") {
		preflist_setops->push_back(make_arity_thresh< CBLQSetOperationsNAry3Dense<2> >(NARY_ARITY_THRESH, CBLQSetOperationsConfig(true)));
		preflist_setops->push_back(make_arity_thresh< CBLQSetOperationsNAry3Dense<3> >(NARY_ARITY_THRESH, CBLQSetOperationsConfig(true)));
		preflist_setops->push_back(make_arity_thresh< CBLQSetOperationsNAry3Dense<4> >(NARY_ARITY_THRESH, CBLQSetOperationsConfig(true)));
		if (verbose) std::cerr << "Using nary3 setops for binmerge" << std::endl;
	} else if (setops_mode == "nary2") {
		preflist_setops->push_back(make_arity_thresh< CBLQSetOperationsNAry2Dense<2> >(NARY_ARITY_THRESH, CBLQSetOperationsConfig(true)));
		preflist_setops->push_back(make_arity_thresh< CBLQSetOperationsNAry2Dense<3> >(NARY_ARITY_THRESH, CBLQSetOperationsConfig(true)));
		preflist_setops->push_back(make_arity_thresh< CBLQSetOperationsNAry2Dense<4> >(NARY_ARITY_THRESH, CBLQSetOperationsConfig(true)));
		if (verbose) std::cerr << "Using nary2 setops for binmerge" << std::endl;
	} else if (setops_mode == "fastunion") {
		preflist_setops->push_back(boost::make_shared< CBLQSetOperationsFast<2> >(CBLQSetOperationsConfig(true)));
		preflist_setops->push_back(boost::make_shared< CBLQSetOperationsFast<3> >(CBLQSetOperationsConfig(true)));
		preflist_setops->push_back(boost::make_shared< CBLQSetOperationsFast<4> >(CBLQSetOperationsConfig(true)));
		if (verbose) std::cerr << "Using fastunion setops for binmerge" << std::endl;
	} else if (setops_mode == "fastnary3") {
		preflist_setops->push_back(make_arity_thresh< CBLQSetOperationsNAry3Fast<2> >(NARYFAST_ARITY_THRESH, CBLQSetOperationsConfig(true)));
		preflist_setops->push_back(make_arity_thresh< CBLQSetOperationsNAry3Fast<3> >(NARYFAST_ARITY_THRESH, CBLQSetOperationsConfig(true)));
		preflist_setops->push_back(make_arity_thresh< CBLQSetOperationsNAry3Fast<4> >(NARYFAST_ARITY_THRESH, CBLQSetOperationsConfig(true)));
		if (verbose) std::cerr << "Using fastnary3 setops for binmerge" << std::endl;
	} else if (setops_mode == "" || setops_mode == "standard" || setops_mode == "basic") {
		preflist_setops->push_back(boost::make_shared< CBLQSetOperations<2> >(CBLQSetOperationsConfig(true)));
		preflist_setops->push_back(boost::make_shared< CBLQSetOperations<3> >(CBLQSetOperationsConfig(true)));
		preflist_setops->push_back(boost::make_shared< CBLQSetOperations<4> >(CBLQSetOperationsConfig(true)));
		if (verbose) std::cerr << "Using standard setops for binmerge" << std::endl;
	} else {
		std::cerr << "WARNING: Unrecognized setops mode: \"" << setops_mode << "\"" << std::endl;
		return nullptr;
	}

	return boost::make_shared< BasicQueryEngine >(preflist_setops, options);
}

static boost::shared_ptr< BitmapQueryEngine > configure_bitmap_query_engine(std::string convert_mode, QueryEngineOptions options, bool verbose) {
	using BitmapPrefListConv = FixedOutputPreferenceListRegionEncodingConverter< BitmapRegionEncoding >;
	boost::shared_ptr< BitmapPrefListConv > preflist_converter = boost::make_shared< BitmapPrefListConv >();

	if (convert_mode == "dfs" || convert_mode == "df") {
		preflist_converter->push_back(boost::make_shared< CBLQToBitmapDFConverter<2> >());
		preflist_converter->push_back(boost::make_shared< CBLQToBitmapDFConverter<3> >());
		preflist_converter->push_back(boost::make_shared< CBLQToBitmapDFConverter<4> >());
		if (verbose) std::cerr << "Using depth-first CBLQ-to-bitmap conversion" << std::endl;
	} else if (convert_mode == "" || convert_mode == "standard" || convert_mode == "basic") {
		preflist_converter->push_back(boost::make_shared< CBLQToBitmapConverter<2> >());
		preflist_converter->push_back(boost::make_shared< CBLQToBitmapConverter<3> >());
		preflist_converter->push_back(boost::make_shared< CBLQToBitmapConverter<4> >());
		if (verbose) std::cerr << "Using standard (breadth-first) CBLQ-to-bitmap conversion" << std::endl;
	} else {
		std::cerr << "WARNING: Unrecognized bitmap converter mode: \"" << convert_mode << "\"" << std::endl;
		return nullptr;
	}

	return boost::make_shared< BitmapQueryEngine >(preflist_converter, boost::make_shared< BitmapSetOperations >(BitmapSetOperationsConfig()), options);
}

boost::shared_ptr< QueryEngine > configure_query_engine(std::string qe_mode, std::string setops_mode, std::string convert_mode, QueryEngineOptions options, bool verbose) {
	std::cerr << "QueryEngineOptions:" << std::endl;
	options.dump(std::cerr);
	if (qe_mode == "bitmap") {
		if (verbose) std::cerr << "Using bitmap-based query engine" << std::endl;
		return configure_bitmap_query_engine(convert_mode, options, verbose);
	} else if (qe_mode == "" || qe_mode == "standard" || qe_mode == "basic" || qe_mode == "setops") {
		if (verbose) std::cerr << "Using setops-based query engine" << std::endl;
		return configure_basic_query_engine(setops_mode, options, verbose);
	} else {
		std::cerr << "WARNING: Unrecognized QueryEngine mode: \"" << qe_mode << "\"" << std::endl;
		return nullptr;
	}
}

