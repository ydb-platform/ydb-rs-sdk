#include "result.h"

#include <ydb-cpp-sdk/client/resources/ydb_resources.h>

namespace sdk_compare {

void to_json(nlohmann::json& value, const TopicMetrics& metrics) {
    value = nlohmann::json{
        {"topic.write_ack", metrics.write_ack},
        {"topic.end_to_end", metrics.end_to_end},
        {"topic.commit_ack", metrics.commit_ack},
        {"write_messages_per_second", metrics.write_messages_per_second},
        {"write_bytes_per_second", metrics.write_bytes_per_second},
        {"read_messages_per_second", metrics.read_messages_per_second},
        {"read_bytes_per_second", metrics.read_bytes_per_second},
    };
}

void to_json(nlohmann::json& value, const BenchmarkResult& result) {
    value = nlohmann::json{
        {"scenario", result.scenario},
        {"implementation", result.implementation},
        {"metrics", result.metrics},
    };
}

Implementation cpp_implementation() {
    return {
        .language = "cpp",
        .sdk_version = NYdb::GetSdkSemver(),
#ifdef NDEBUG
        .build_profile = "release",
#else
        .build_profile = "debug",
#endif
    };
}

}  // namespace sdk_compare
