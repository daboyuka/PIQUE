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
 * timing.h
 *
 *  Created on: Jun 20, 2014
 *      Author: David A. Boyuka II
 */
#ifndef TIMING_H_
#define TIMING_H_

#include <utility> // std::move
#include <boost/timer/timer.hpp>

class TimeBlock {
private:
	static constexpr double SEC_PER_NANO = 1e-9;
public:
	TimeBlock(double *add_wallclock, double *add_cpu = nullptr) : add_wallclock(add_wallclock), add_cpu(add_cpu), timer() {}
	~TimeBlock() {
		timer.stop();
		const boost::timer::cpu_times times = timer.elapsed();
		if (add_wallclock)
			*add_wallclock	+= times.wall * SEC_PER_NANO;
		if (add_cpu)
			*add_cpu 		+= times.user * SEC_PER_NANO;
	}
	TimeBlock(const TimeBlock &) = delete;
	TimeBlock(TimeBlock &&other) : add_wallclock(other.add_wallclock), add_cpu(other.add_cpu), timer(std::move(other.timer)) {
		other.add_wallclock = nullptr;
		other.add_cpu = nullptr;
	}

	TimeBlock & operator=(const TimeBlock &) = delete;
	TimeBlock & operator=(TimeBlock &&other) {
		this->timer = std::move(other.timer);
		this->add_wallclock = other.add_wallclock;
		this->add_cpu = other.add_cpu;
		other.add_wallclock = nullptr;
		other.add_cpu = nullptr;
		return *this;
	}
private:
	double *add_wallclock, *add_cpu;
	boost::timer::cpu_timer timer;
};

#define TIME_BLOCK_BEGIN(wc, cpu) { TimeBlock tb = TimeBlock(wc, cpu);
#define TIME_BLOCK_END() }

#endif /* TIMING_H_ */
