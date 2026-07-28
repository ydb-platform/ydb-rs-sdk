#pragma once

#include <ydb-cpp-sdk/client/topic/client.h>

#include <memory>
#include <string>
#include <vector>

#include "core/metrics.h"
#include "core/scenario.h"
#include "core/schedule.h"

namespace sdk_compare {

class WriterWorker;

struct WriterMetrics {
    LatencyMetric write_ack;
};

class TopicWriters {
public:
    TopicWriters(NYdb::NTopic::TTopicClient& topic_client,
                 const std::string& topic_path,
                 const TopicWorkload& workload);
    ~TopicWriters();

    TopicWriters(const TopicWriters&) = delete;
    TopicWriters& operator=(const TopicWriters&) = delete;
    TopicWriters(TopicWriters&&) = delete;
    TopicWriters& operator=(TopicWriters&&) = delete;

    NThreading::TFuture<WriterMetrics> run(
        BenchmarkSchedule schedule,
        NThreading::TFuture<void> measurement_ended);

private:
    void shutdown() noexcept;

    std::vector<std::unique_ptr<WriterWorker>> writers_;
    NThreading::TFuture<WriterMetrics> result_;
    bool started_ = false;
};

}  // namespace sdk_compare
