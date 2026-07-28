#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/topic/client.h>

#include <string>

#include "core/scenario.h"

namespace sdk_compare {

class TopicFixture {
public:
    TopicFixture(const Execution& execution, const TopicWorkload& workload);
    ~TopicFixture();

    TopicFixture(const TopicFixture&) = delete;
    TopicFixture& operator=(const TopicFixture&) = delete;
    TopicFixture(TopicFixture&&) = delete;
    TopicFixture& operator=(TopicFixture&&) = delete;

    NYdb::NTopic::TTopicClient& topic_client();
    const std::string& topic_path() const;

private:
    NYdb::TDriver driver_;
    NYdb::NTopic::TTopicClient topic_client_;
    std::string topic_path_;
    bool topic_created_ = false;
};

}  // namespace sdk_compare
