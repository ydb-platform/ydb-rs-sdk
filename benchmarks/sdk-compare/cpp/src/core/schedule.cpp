#include "schedule.h"

#include <limits>
#include <stdexcept>
#include <string_view>

namespace sdk_compare {
namespace {

BenchmarkSchedule::TimePoint add_seconds(BenchmarkSchedule::TimePoint instant,
                                         std::uint64_t seconds,
                                         std::string_view description) {
    using Clock = BenchmarkSchedule::Clock;
    using Seconds = std::chrono::seconds;

    if (seconds >
        static_cast<std::uint64_t>(std::numeric_limits<Seconds::rep>::max())) {
        throw std::runtime_error(std::string(description) + " overflowed");
    }

    const auto duration =
        std::chrono::duration_cast<Clock::duration>(Seconds(seconds));
    if (duration > Clock::time_point::max() - instant) {
        throw std::runtime_error(std::string(description) + " overflowed");
    }
    return instant + duration;
}

}  // namespace

BenchmarkSchedule BenchmarkSchedule::start(const Execution& execution) {
    const auto origin = Clock::now();
    const auto measurement_start =
        add_seconds(origin, execution.warmup_seconds, "warm-up deadline");
    const auto measurement_end = add_seconds(measurement_start,
                                             execution.measurement_seconds,
                                             "measurement deadline");
    const auto completion_deadline =
        add_seconds(measurement_end,
                    execution.drain_timeout_seconds,
                    "measurement drain deadline");
    return {origin, measurement_start, measurement_end, completion_deadline};
}

BenchmarkSchedule::BenchmarkSchedule(TimePoint origin,
                                     TimePoint measurement_start,
                                     TimePoint measurement_end,
                                     TimePoint completion_deadline)
    : origin_(origin)
    , measurement_start_(measurement_start)
    , measurement_end_(measurement_end)
    , completion_deadline_(completion_deadline) {}

bool BenchmarkSchedule::is_measurement_instant(TimePoint instant) const {
    return instant >= measurement_start_ && instant < measurement_end_;
}

std::uint64_t BenchmarkSchedule::ns_at(TimePoint instant) const {
    if (instant < origin_) {
        throw std::runtime_error("benchmark instant is before schedule origin");
    }
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::nanoseconds>(instant - origin_)
            .count();
    if (elapsed < 0) {
        throw std::runtime_error(
            "benchmark schedule produced a negative timestamp");
    }
    return static_cast<std::uint64_t>(elapsed);
}

std::uint64_t BenchmarkSchedule::now_ns() const { return ns_at(Clock::now()); }

BenchmarkSchedule::TimePoint BenchmarkSchedule::measurement_start() const {
    return measurement_start_;
}

BenchmarkSchedule::TimePoint BenchmarkSchedule::measurement_end() const {
    return measurement_end_;
}

BenchmarkSchedule::TimePoint BenchmarkSchedule::completion_deadline() const {
    return completion_deadline_;
}

TDuration remaining_timeout(BenchmarkSchedule::TimePoint deadline) {
    const auto now = BenchmarkSchedule::Clock::now();
    if (now >= deadline) {
        return TDuration::Zero();
    }
    const auto micros =
        std::chrono::duration_cast<std::chrono::microseconds>(deadline - now)
            .count();
    return TDuration::MicroSeconds(static_cast<std::uint64_t>(micros));
}

}  // namespace sdk_compare
