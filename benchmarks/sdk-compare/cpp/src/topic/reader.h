#pragma once

#include <library/cpp/threading/future/future.h>
#include <ydb-cpp-sdk/client/topic/client.h>

#include <memory>
#include <string>
#include <vector>

#include "core/metrics.h"
#include "core/scenario.h"
#include "core/schedule.h"

namespace sdk_compare {

class ReaderWorker;

struct ReaderMetrics {
    LatencyMetric end_to_end;
    LatencyMetric commit_ack;
};

class TopicReaders {
public:
    TopicReaders(NYdb::NTopic::TTopicClient& topic_client,
                 const std::string& topic_path,
                 const TopicWorkload& workload);
    ~TopicReaders();

    TopicReaders(const TopicReaders&) = delete;
    TopicReaders& operator=(const TopicReaders&) = delete;
    TopicReaders(TopicReaders&&) = delete;
    TopicReaders& operator=(TopicReaders&&) = delete;

    NThreading::TFuture<ReaderMetrics> run(
        BenchmarkSchedule schedule,
        NThreading::TFuture<void> measurement_ended);

private:
    void shutdown() noexcept;

    std::vector<std::unique_ptr<ReaderWorker>> readers_;
    NThreading::TFuture<ReaderMetrics> result_;
    bool started_ = false;
};

}  // namespace sdk_compare
