#include "metrics.h"

#include <algorithm>
#include <stdexcept>

namespace sdk_compare {
namespace {

inline constexpr std::int64_t lowest_latency_us = 1;
inline constexpr std::int64_t highest_latency_us = 300'000'000;
inline constexpr int significant_digits = 3;

std::uint64_t as_unsigned(std::int64_t value) {
    if (value < 0) {
        throw std::runtime_error("latency histogram returned a negative value");
    }
    return static_cast<std::uint64_t>(value);
}

}  // namespace

void to_json(nlohmann::json& value, const LatencyMetric& metric) {
    value = nlohmann::json{
        {"count", metric.count},
        {"latency_us",
         metric.latency_us ? nlohmann::json(*metric.latency_us)
                           : nlohmann::json(nullptr)},
    };
}

LatencyRecorder::LatencyRecorder() {
    hdr_histogram* histogram = nullptr;
    if (hdr_init(lowest_latency_us,
                 highest_latency_us,
                 significant_digits,
                 &histogram) != 0) {
        throw std::runtime_error("failed to create latency histogram");
    }
    histogram_.reset(histogram);
}

void LatencyRecorder::HistogramDeleter::operator()(
    hdr_histogram* histogram) const {
    hdr_close(histogram);
}

void LatencyRecorder::record(std::chrono::steady_clock::duration latency) {
    const auto micros =
        std::chrono::duration_cast<std::chrono::microseconds>(latency).count();
    const auto value = std::max<std::int64_t>(micros, lowest_latency_us);
    if (!hdr_record_value(histogram_.get(), value)) {
        throw std::runtime_error("latency " + std::to_string(value) +
                                 " us is outside histogram bounds");
    }
}

void LatencyRecorder::merge(const LatencyRecorder& other) {
    if (this == &other) {
        throw std::runtime_error(
            "cannot merge a latency histogram into itself");
    }

    const auto dropped = hdr_add(histogram_.get(), other.histogram_.get());
    if (dropped != 0) {
        throw std::runtime_error(
            "failed to merge latency histograms: " + std::to_string(dropped) +
            " values were dropped");
    }
}

LatencyMetric LatencyRecorder::summary() const {
    const auto count = as_unsigned(histogram_->total_count);
    if (count == 0) {
        return LatencyMetric{
            .count = count,
            .latency_us = std::nullopt,
        };
    }
    return LatencyMetric{
        .count = count,
        .latency_us =
            LatencySummary{
                .min = as_unsigned(hdr_min(histogram_.get())),
                .max = as_unsigned(hdr_max(histogram_.get())),
                .mean = hdr_mean(histogram_.get()),
                .p50 = as_unsigned(
                    hdr_value_at_percentile(histogram_.get(), 50.0)),
                .p95 = as_unsigned(
                    hdr_value_at_percentile(histogram_.get(), 95.0)),
                .p99 = as_unsigned(
                    hdr_value_at_percentile(histogram_.get(), 99.0)),
                .p99_9 = as_unsigned(
                    hdr_value_at_percentile(histogram_.get(), 99.9)),
            },
    };
}

}  // namespace sdk_compare
