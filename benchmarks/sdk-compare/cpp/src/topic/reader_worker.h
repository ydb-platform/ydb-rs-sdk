#pragma once

#include <library/cpp/threading/future/future.h>
#include <ydb-cpp-sdk/client/topic/client.h>
#include <ydb-cpp-sdk/client/topic/read_session.h>

#include <cstddef>
#include <memory>
#include <string>

#include "core/metrics.h"
#include "core/scenario.h"
#include "core/schedule.h"

namespace sdk_compare {

struct ReaderMeasurements {
    LatencyRecorder end_to_end;
    LatencyRecorder commit_ack;
};

class ReaderWorker {
public:
    ReaderWorker(NYdb::NTopic::TTopicClient& topic_client,
                 const std::string& topic_path,
                 const TopicWorkload& workload,
                 std::size_t reader_id);
    ~ReaderWorker();

    ReaderWorker(const ReaderWorker&) = delete;
    ReaderWorker& operator=(const ReaderWorker&) = delete;
    ReaderWorker(ReaderWorker&&) = delete;
    ReaderWorker& operator=(ReaderWorker&&) = delete;

    NThreading::TFuture<ReaderMeasurements> start(
        BenchmarkSchedule schedule,
        NThreading::TFuture<void> measurement_ended);
    void shutdown() noexcept;

private:
    NThreading::TFuture<ReaderMeasurements> run(
        BenchmarkSchedule schedule,
        NThreading::TFuture<void> measurement_ended);

    const std::size_t id_;
    std::shared_ptr<NYdb::NTopic::IReadSession> session_;
    NThreading::TFuture<ReaderMeasurements> result_;
    bool started_ = false;
    bool stopped_ = false;
};

}  // namespace sdk_compare
