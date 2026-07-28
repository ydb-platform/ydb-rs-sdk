#include "scenario.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <utility>

namespace sdk_compare {
namespace {

using Json = nlohmann::json;

std::string workload_kind(const Json& value) {
    if (!value.is_object()) {
        throw ScenarioError("workload must be a JSON object");
    }

    const auto& kind = value.at("kind");
    if (!kind.is_string()) {
        throw ScenarioError("workload.kind must be a string");
    }
    return kind.get<std::string>();
}

bool is_blank(std::string_view value) {
    return std::all_of(value.begin(), value.end(), [](unsigned char character) {
        return std::isspace(character) != 0;
    });
}

void require_positive(std::uint64_t value, std::string_view path) {
    if (value == 0) {
        throw ScenarioError(std::string(path) + " must be greater than zero");
    }
}

void validate(const Execution& execution) {
    require_positive(execution.worker_threads, "execution.worker_threads");
    require_positive(execution.measurement_seconds,
                     "execution.measurement_seconds");
    require_positive(execution.drain_timeout_seconds,
                     "execution.drain_timeout_seconds");
}

void validate(const TopicWorkload& workload) {
    if (is_blank(workload.topic_name)) {
        throw ScenarioError("workload.topic_name must not be empty");
    }
    if (is_blank(workload.consumer_name)) {
        throw ScenarioError("workload.consumer_name must not be empty");
    }
    require_positive(workload.partition_count, "workload.partition_count");
    require_positive(workload.writers_per_partition,
                     "workload.writers_per_partition");
    require_positive(workload.reader_count, "workload.reader_count");
    if (workload.message_size_bytes < topic_payload_header_size_bytes) {
        throw ScenarioError("workload.message_size_bytes must be at least " +
                            std::to_string(topic_payload_header_size_bytes));
    }
    require_positive(workload.max_in_flight_per_writer,
                     "workload.max_in_flight_per_writer");
    if (workload.write_batch_max_messages != 1) {
        throw ScenarioError(
            "workload.write_batch_max_messages must be 1 for comparable SDK "
            "writes");
    }
    require_positive(workload.write_batch_max_delay_ms,
                     "workload.write_batch_max_delay_ms");
    if (workload.partition_write_speed_bytes_per_second <= 0) {
        throw ScenarioError(
            "workload.partition_write_speed_bytes_per_second must be greater "
            "than zero");
    }
}

void validate(const Workload& workload) {
    std::visit([](const auto& value) { validate(value); }, workload);
}

void validate(const Scenario& scenario) {
    if (is_blank(scenario.name)) {
        throw ScenarioError("scenario.name must not be empty");
    }
    validate(scenario.execution);
    validate(scenario.workload);
}

}  // namespace

void to_json(Json& value, const Workload& workload) {
    value = std::get<TopicWorkload>(workload);
    value["kind"] = "topic";
}

void from_json(const Json& value, Workload& workload) {
    const auto kind = workload_kind(value);
    if (kind == "topic") {
        workload = value.get<TopicWorkload>();
        return;
    }
    throw ScenarioError("unsupported workload.kind '" + kind + "'");
}

Scenario parse_scenario(std::string_view input) {
    try {
        const auto document = Json::parse(input);
        auto scenario = document.get<Scenario>();
        validate(scenario);
        return scenario;
    } catch (const ScenarioError&) {
        throw;
    } catch (const Json::exception& error) {
        throw ScenarioError(std::string("failed to parse scenario JSON: ") +
                            error.what());
    }
}

Scenario load_scenario(const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input.is_open()) {
        throw ScenarioError("failed to read scenario file " + path.string());
    }

    std::ostringstream contents;
    contents << input.rdbuf();
    if (input.bad()) {
        throw ScenarioError("failed to read scenario file " + path.string());
    }

    try {
        return parse_scenario(std::move(contents).str());
    } catch (const ScenarioError& error) {
        throw ScenarioError("failed to load scenario file " + path.string() +
                            ": " + error.what());
    }
}

}  // namespace sdk_compare
