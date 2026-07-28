#include "fixture.h"

#include <ydb-cpp-sdk/client/helpers/helpers.h>

#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace sdk_compare {
namespace {

constexpr char connection_string_environment_variable[] =
    "YDB_CONNECTION_STRING";

NYdb::TDriverConfig driver_config_from_environment(const Execution& execution) {
    const auto* connection_string =
        std::getenv(connection_string_environment_variable);
    if (connection_string == nullptr || connection_string[0] == '\0') {
        throw std::runtime_error(
            std::string(connection_string_environment_variable) +
            " is not set");
    }

    auto config = NYdb::CreateFromEnvironment(connection_string);
    config.SetClientThreadsNum(execution.worker_threads);
    if (config.GetDatabase().empty()) {
        throw std::runtime_error(
            std::string(connection_string_environment_variable) +
            " does not contain a database path");
    }
    return config;
}

std::string build_topic_path(const NYdb::TDriver& driver,
                             const TopicWorkload& workload) {
    auto database = driver.GetConfig().GetDatabase();
    while (database.size() > 1 && database.back() == '/') {
        database.pop_back();
    }
    return database + '/' + workload.topic_name;
}

std::string status_error(std::string operation, const NYdb::TStatus& status) {
    operation += " failed with YDB status ";
    operation += std::to_string(static_cast<std::size_t>(status.GetStatus()));

    const auto issues = status.GetIssues().ToString(true);
    if (!issues.empty()) {
        operation += ": ";
        operation.append(issues.data(), issues.size());
    }
    return operation;
}

NYdb::NTopic::TCreateTopicSettings topic_settings(
    const TopicWorkload& workload) {
    const auto partition_count =
        static_cast<std::uint64_t>(workload.partition_count);
    auto settings = NYdb::NTopic::TCreateTopicSettings();
    settings.PartitioningSettings(partition_count, partition_count)
        .SetSupportedCodecs({NYdb::NTopic::ECodec::RAW})
        .PartitionWriteSpeedBytesPerSecond(static_cast<std::uint64_t>(
            workload.partition_write_speed_bytes_per_second));
    settings.BeginAddConsumer()
        .ConsumerName(workload.consumer_name)
        .Important(true)
        .EndAddConsumer();
    return settings;
}

}  // namespace

TopicFixture::TopicFixture(const Execution& execution,
                           const TopicWorkload& workload)
    : driver_(driver_config_from_environment(execution))
    , topic_client_(driver_)
    , topic_path_(build_topic_path(driver_, workload)) {
    const auto status =
        topic_client_.CreateTopic(topic_path_, topic_settings(workload))
            .GetValueSync();
    if (!status.IsSuccess()) {
        throw std::runtime_error(
            status_error("create topic " + topic_path_, status));
    }
    topic_created_ = true;
}

TopicFixture::~TopicFixture() {
    if (topic_created_) {
        try {
            const auto status =
                topic_client_.DropTopic(topic_path_).GetValueSync();
            if (!status.IsSuccess()) {
                std::cerr << "warning: "
                          << status_error("drop benchmark topic " + topic_path_,
                                          status)
                          << '\n';
            }
        } catch (const std::exception& error) {
            std::cerr << "warning: failed to drop benchmark topic "
                      << topic_path_ << ": " << error.what() << '\n';
        }
    }

    try {
        driver_.Stop(true);
    } catch (const std::exception& error) {
        std::cerr << "warning: failed to stop YDB driver: " << error.what()
                  << '\n';
    }
}

NYdb::NTopic::TTopicClient& TopicFixture::topic_client() {
    return topic_client_;
}

const std::string& TopicFixture::topic_path() const { return topic_path_; }

}  // namespace sdk_compare
