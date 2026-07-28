#pragma once

#include <library/cpp/threading/future/future.h>
#include <ydb-cpp-sdk/client/topic/client.h>
#include <ydb-cpp-sdk/client/topic/write_events.h>
#include <ydb-cpp-sdk/client/topic/write_session.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "core/metrics.h"
#include "core/scenario.h"
#include "core/schedule.h"

namespace sdk_compare {

struct WriterMeasurements {
    LatencyRecorder write_ack;
};

class WriterWorker {
public:
    WriterWorker(NYdb::NTopic::TTopicClient& topic_client,
                 const std::string& topic_path,
                 const TopicWorkload& workload,
                 std::uint32_t partition_id,
                 std::size_t writer_index,
                 std::size_t writer_id);
    ~WriterWorker();

    WriterWorker(const WriterWorker&) = delete;
    WriterWorker& operator=(const WriterWorker&) = delete;
    WriterWorker(WriterWorker&&) = delete;
    WriterWorker& operator=(WriterWorker&&) = delete;

    NThreading::TFuture<WriterMeasurements> start(
        BenchmarkSchedule schedule,
        NThreading::TFuture<void> measurement_ended);
    void shutdown() noexcept;

private:
    NThreading::TFuture<WriterMeasurements> run(
        BenchmarkSchedule schedule,
        NThreading::TFuture<void> measurement_ended,
        NYdb::NTopic::TContinuationToken initial_token);

    const std::size_t id_;
    const std::size_t message_size_bytes_;
    const std::size_t max_in_flight_;
    std::shared_ptr<NYdb::NTopic::IWriteSession> session_;
    std::optional<NYdb::NTopic::TContinuationToken> initial_token_;
    NThreading::TFuture<WriterMeasurements> result_;
    bool started_ = false;
    bool stopped_ = false;
};

}  // namespace sdk_compare
