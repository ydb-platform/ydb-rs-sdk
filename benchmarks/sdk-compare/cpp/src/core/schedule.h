#pragma once

#include <util/datetime/base.h>

#include <chrono>
#include <cstdint>

#include "scenario.h"

namespace sdk_compare {

class BenchmarkSchedule {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    static BenchmarkSchedule start(const Execution& execution);

    bool is_measurement_instant(TimePoint instant) const;
    std::uint64_t ns_at(TimePoint instant) const;
    std::uint64_t now_ns() const;

    TimePoint measurement_start() const;
    TimePoint measurement_end() const;
    TimePoint completion_deadline() const;

private:
    BenchmarkSchedule(TimePoint origin,
                      TimePoint measurement_start,
                      TimePoint measurement_end,
                      TimePoint completion_deadline);

    TimePoint origin_;
    TimePoint measurement_start_;
    TimePoint measurement_end_;
    TimePoint completion_deadline_;
};

TDuration remaining_timeout(BenchmarkSchedule::TimePoint deadline);

}  // namespace sdk_compare
