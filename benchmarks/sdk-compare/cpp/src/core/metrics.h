#pragma once

#include <hdr/hdr_histogram.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>

namespace sdk_compare {

struct LatencySummary {
    std::uint64_t min;
    std::uint64_t max;
    double mean;
    std::uint64_t p50;
    std::uint64_t p95;
    std::uint64_t p99;
    std::uint64_t p99_9;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(
        LatencySummary, min, max, mean, p50, p95, p99, p99_9)
};

struct LatencyMetric {
    std::uint64_t count;
    std::optional<LatencySummary> latency_us;
};

void to_json(nlohmann::json& value, const LatencyMetric& metric);

// A recorder has one owner. Merge and summarize it only after that owner stops.
class LatencyRecorder {
public:
    LatencyRecorder();

    void record(std::chrono::steady_clock::duration latency);
    void merge(const LatencyRecorder& other);
    LatencyMetric summary() const;

private:
    struct HistogramDeleter {
        void operator()(hdr_histogram* histogram) const;
    };

    std::unique_ptr<hdr_histogram, HistogramDeleter> histogram_;
};

}  // namespace sdk_compare
