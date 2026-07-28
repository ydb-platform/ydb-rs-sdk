#pragma once

#include <nlohmann/json.hpp>
#include <string>

#include "metrics.h"
#include "scenario.h"

namespace sdk_compare {

struct Implementation {
    std::string language;
    std::string sdk_version;
    std::string build_profile;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(Implementation,
                                   language,
                                   sdk_version,
                                   build_profile)
};

struct TopicMetrics {
    LatencyMetric write_ack;
    LatencyMetric end_to_end;
    LatencyMetric commit_ack;
    double write_messages_per_second;
    double write_bytes_per_second;
    double read_messages_per_second;
    double read_bytes_per_second;
};

void to_json(nlohmann::json& value, const TopicMetrics& metrics);

struct BenchmarkResult {
    Scenario scenario;
    Implementation implementation;
    TopicMetrics metrics;
};

void to_json(nlohmann::json& value, const BenchmarkResult& result);

Implementation cpp_implementation();

}  // namespace sdk_compare
