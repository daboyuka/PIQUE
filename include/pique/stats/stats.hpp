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
 * stats.hpp
 *
 *  Created on: Sep 15, 2014
 *      Author: David A. Boyuka II
 */
#ifndef STATS_HPP_
#define STATS_HPP_

#include <cstdint>
#include <boost/serialization/level.hpp>
#include "pique/util/timing.hpp"

class AbstractStats {
public:
	virtual ~AbstractStats() {}
};

struct Add { template<typename ArgT> void operator()(ArgT &v1, ArgT v2) { v1 += v2; } };
struct Sub { template<typename ArgT> void operator()(ArgT &v1, ArgT v2) { v1 -= v2; } };
struct Set { template<typename ArgT> void operator()(ArgT &v1, ArgT v2) { v1 =  v2; } };
struct Clr { template<typename ArgT> void operator()(ArgT &v1, ArgT v2) { v1 =  0;  } };

template<typename DerivedT>
class BaseStats : public AbstractStats {
public:
	virtual ~BaseStats() {}

	DerivedT & operator+=(const DerivedT &other)       { DerivedT &dthis = static_cast< DerivedT & >(*this); dthis.combine(other, Add()); return dthis; }
	DerivedT & operator-=(const DerivedT &other)       { DerivedT &dthis = static_cast< DerivedT & >(*this); dthis.combine(other, Sub()); return dthis; }
	DerivedT & operator= (const DerivedT &other)       { DerivedT &dthis = static_cast< DerivedT & >(*this); dthis.combine(other, Set()); return dthis; }

	DerivedT   operator+ (const DerivedT &other) const { const DerivedT &dthis = static_cast< const DerivedT & >(*this); DerivedT out = dthis; out.combine(other, Add()); return out; }
	DerivedT   operator- (const DerivedT &other) const { const DerivedT &dthis = static_cast< const DerivedT & >(*this); DerivedT out = dthis; out.combine(other, Sub()); return out; }

	// template<typename CombineFn> void combine(const DerivedT &other, CombineFn combine) { combine(this.fieldA, other.fieldA); ... }
};

struct TimeStats : public BaseStats< TimeStats > {
public:
	TimeStats() {}
	TimeStats(double time, double cpuTime) : time(time), cpuTime(cpuTime) {}
	virtual ~TimeStats() {}

	template<typename CombineFn> void combine(const TimeStats &other, CombineFn combine) {
		combine(this->time, other.time);
		combine(this->cpuTime, other.cpuTime);
	}

	TimeBlock open_time_block() { return TimeBlock(&time, &cpuTime); }

	double time{0}, cpuTime{0};
};

#define TIME_STATS_TIME_BEGIN(ts) { TimeBlock tb = (ts).open_time_block();
#define TIME_STATS_TIME_END()     }

struct IOStats : public BaseStats< IOStats > {
	virtual ~IOStats() {}

	template<typename CombineFn> void combine(const IOStats &other, CombineFn combine) {
		combine(this->read_time,   other.read_time  );
		combine(this->write_time,  other.write_time );
		combine(this->read_bytes,  other.read_bytes );
		combine(this->write_bytes, other.write_bytes);
		combine(this->read_seeks,  other.read_seeks );
		combine(this->write_seeks, other.write_seeks);
	}

	TimeBlock open_read_time_block() { return TimeBlock(&read_time); }
	TimeBlock open_write_time_block() { return TimeBlock(&write_time); }

	TimeStats get_read_as_time_stats() const { return TimeStats(read_time, 0); }
	TimeStats get_write_as_time_stats() const { return TimeStats(write_time, 0); }

	double read_time{0}, write_time{0};
	uint64_t read_bytes{0}, write_bytes{0};
	uint64_t read_seeks{0}, write_seeks{0};
};

#define IO_STATS_READ_TIME_BEGIN(ios)  { TimeBlock tb = (ios).open_read_time_block();
#define IO_STATS_READ_TIME_END()       }
#define IO_STATS_WRITE_TIME_BEGIN(ios) { TimeBlock tb = (ios).open_write_time_block();
#define IO_STATS_WRITE_TIME_END()      }

// Macro to easily define a serialization routine for a stats class
#define DEFINE_STATS_SERIALIZE(statstype, statsname, serializecode) \
	namespace boost { namespace serialization { \
	template<typename Archive> void serialize(Archive &ar, statstype &statsname, const unsigned int version) { \
		ar & serializecode; \
	}}} \
	BOOST_CLASS_IMPLEMENTATION(statstype, boost::serialization::object_serializable) /* no version information serialized */

// Define serialization for TimeStats and IOStats
DEFINE_STATS_SERIALIZE(TimeStats, stats, stats.time & stats.cpuTime);
DEFINE_STATS_SERIALIZE(IOStats, stats, stats.read_time & stats.write_time & stats.read_bytes & stats.write_bytes & stats.read_seeks & stats.write_seeks);

#endif /* STATS_HPP_ */
