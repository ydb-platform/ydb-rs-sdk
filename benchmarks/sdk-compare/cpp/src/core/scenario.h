#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>

namespace sdk_compare {

inline constexpr std::size_t topic_payload_header_size_bytes =
    sizeof(std::uint64_t);

struct Execution {
    std::size_t worker_threads;
    std::uint64_t warmup_seconds;
    std::uint64_t measurement_seconds;
    std::uint64_t drain_timeout_seconds;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(Execution,
                                   worker_threads,
                                   warmup_seconds,
                                   measurement_seconds,
                                   drain_timeout_seconds)
};

struct TopicWorkload {
    std::string topic_name;
    std::string consumer_name;
    std::uint32_t partition_count;
    std::size_t writers_per_partition;
    std::size_t reader_count;
    std::size_t message_size_bytes;
    std::size_t max_in_flight_per_writer;
    std::size_t write_batch_max_messages;
    std::uint64_t write_batch_max_delay_ms;
    std::int64_t partition_write_speed_bytes_per_second;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(TopicWorkload,
                                   topic_name,
                                   consumer_name,
                                   partition_count,
                                   writers_per_partition,
                                   reader_count,
                                   message_size_bytes,
                                   max_in_flight_per_writer,
                                   write_batch_max_messages,
                                   write_batch_max_delay_ms,
                                   partition_write_speed_bytes_per_second)
};

using Workload = std::variant<TopicWorkload>;

void to_json(nlohmann::json& value, const Workload& workload);
void from_json(const nlohmann::json& value, Workload& workload);

struct Scenario {
    std::string name;
    Execution execution;
    Workload workload;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(Scenario, name, execution, workload)
};

class ScenarioError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

Scenario parse_scenario(std::string_view input);
Scenario load_scenario(const std::filesystem::path& path);

}  // namespace sdk_compare
