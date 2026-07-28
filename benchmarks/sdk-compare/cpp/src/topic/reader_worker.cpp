#include "reader_worker.h"

#include <library/cpp/threading/future/core/coroutine_traits.h>
#include <library/cpp/threading/future/wait/wait.h>
#include <ydb-cpp-sdk/client/topic/read_events.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "core/payload.h"

namespace sdk_compare {
namespace {

using ReadEvent = NYdb::NTopic::TReadSessionEvent;
using StopConfirmation = std::function<void()>;

struct PendingCommit {
    std::uint64_t target_offset;
    BenchmarkSchedule::TimePoint started;
    bool measured;
};

struct PartitionState {
    std::deque<PendingCommit> pending_commits;
    std::optional<StopConfirmation> stop_confirmation;
};

NYdb::NTopic::TReadSessionSettings reader_settings(
    const std::string& topic_path, const TopicWorkload& workload) {
    auto settings = NYdb::NTopic::TReadSessionSettings();
    settings.ConsumerName(workload.consumer_name)
        .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path(topic_path))
        .Decompress(true)
        .DirectRead(false);
    return settings;
}

class ReaderPipeline {
public:
    ReaderPipeline(BenchmarkSchedule schedule,
                   LatencyRecorder& end_to_end,
                   LatencyRecorder& commit_ack)
        : schedule_(schedule)
        , measurement_start_ns_(schedule_.ns_at(schedule_.measurement_start()))
        , end_to_end_(end_to_end)
        , commit_ack_(commit_ack) {}

    void finish_measurement() { reading_ = false; }

    bool reading() const { return reading_; }

    bool drained() const { return !reading_ && pending_commit_count_ == 0; }

    void on_data(ReadEvent::TDataReceivedEvent& event) {
        if (!reading_) {
            return;
        }

        const auto delivered_at = BenchmarkSchedule::Clock::now();
        if (delivered_at >= schedule_.measurement_end()) {
            finish_measurement();
            return;
        }
        if (event.HasCompressedMessages()) {
            throw std::runtime_error(
                "received compressed messages with decompression enabled");
        }

        const auto delivered_at_ns = schedule_.ns_at(delivered_at);
        bool measured_batch = false;
        std::uint64_t target_offset = 0;
        for (const auto& message : event.GetMessages()) {
            const auto sent_at_ns = read_timestamp(message.GetData());
            target_offset = std::max(
                target_offset,
                message.GetOffset() + message.GetLogicalMessageCount());
            if (sent_at_ns < measurement_start_ns_) {
                continue;
            }
            if (sent_at_ns > delivered_at_ns) {
                throw std::runtime_error(
                    "payload timestamp " + std::to_string(sent_at_ns) +
                    " is ahead of delivery timestamp " +
                    std::to_string(delivered_at_ns) + " for producer " +
                    message.GetProducerId() + ", sequence number " +
                    std::to_string(message.GetSeqNo()) + ", offset " +
                    std::to_string(message.GetOffset()));
            }
            measured_batch = true;
            end_to_end_.record(
                std::chrono::nanoseconds(delivered_at_ns - sent_at_ns));
        }

        const auto partition_session_id =
            event.GetPartitionSession()->GetPartitionSessionId();
        auto& pending = partitions_[partition_session_id].pending_commits;
        const auto commit_started = BenchmarkSchedule::Clock::now();
        pending.push_back(PendingCommit{
            .target_offset = target_offset,
            .started = commit_started,
            .measured = measured_batch,
        });
        ++pending_commit_count_;
        event.Commit();
    }

    void on_commit_acknowledgement(
        ReadEvent::TCommitOffsetAcknowledgementEvent& event) {
        const auto partition_session_id =
            event.GetPartitionSession()->GetPartitionSessionId();
        const auto partition = partitions_.find(partition_session_id);
        if (partition == partitions_.end()) {
            throw std::runtime_error(
                "received commit acknowledgement for unknown partition "
                "session " +
                std::to_string(partition_session_id));
        }

        const auto acknowledged_at = BenchmarkSchedule::Clock::now();
        auto& pending = partition->second.pending_commits;
        while (!pending.empty() &&
               pending.front().target_offset <= event.GetCommittedOffset()) {
            if (pending.front().measured) {
                commit_ack_.record(acknowledged_at - pending.front().started);
            }
            pending.pop_front();
            --pending_commit_count_;
        }

        if (pending.empty() && partition->second.stop_confirmation) {
            auto confirmation = std::exchange(
                partition->second.stop_confirmation, std::nullopt);
            (*confirmation)();
        }
    }

    void on_start_partition(ReadEvent::TStartPartitionSessionEvent& event) {
        event.Confirm();
    }

    void on_stop_partition(ReadEvent::TStopPartitionSessionEvent& event) {
        const auto partition_session_id =
            event.GetPartitionSession()->GetPartitionSessionId();
        auto& partition = partitions_[partition_session_id];
        if (partition.stop_confirmation) {
            throw std::runtime_error(
                "received another stop event for partition session " +
                std::to_string(partition_session_id));
        }

        auto confirmation = [event]() mutable { event.Confirm(); };
        if (partition.pending_commits.empty()) {
            confirmation();
        } else {
            partition.stop_confirmation = std::move(confirmation);
        }
    }

    void on_end_partition(ReadEvent::TEndPartitionSessionEvent& event) {
        event.Confirm();
    }

    void on_partition_closed(ReadEvent::TPartitionSessionClosedEvent& event) {
        const auto partition_session_id =
            event.GetPartitionSession()->GetPartitionSessionId();
        const auto partition = partitions_.find(partition_session_id);
        if (partition != partitions_.end() &&
            !partition->second.pending_commits.empty()) {
            throw std::runtime_error("partition session " +
                                     std::to_string(partition_session_id) +
                                     " closed with pending commits");
        }
        partitions_.erase(partition_session_id);
    }

private:
    BenchmarkSchedule schedule_;
    const std::uint64_t measurement_start_ns_;
    LatencyRecorder& end_to_end_;
    LatencyRecorder& commit_ack_;
    std::unordered_map<std::uint64_t, PartitionState> partitions_;
    std::size_t pending_commit_count_ = 0;
    bool reading_ = true;
};

}  // namespace

ReaderWorker::ReaderWorker(NYdb::NTopic::TTopicClient& topic_client,
                           const std::string& topic_path,
                           const TopicWorkload& workload,
                           std::size_t reader_id)
    : id_(reader_id)
    , session_(topic_client.CreateReadSession(
          reader_settings(topic_path, workload))) {
    if (!session_) {
        throw std::runtime_error("failed to create reader " +
                                 std::to_string(id_));
    }
}

ReaderWorker::~ReaderWorker() { shutdown(); }

NThreading::TFuture<ReaderMeasurements> ReaderWorker::start(
    BenchmarkSchedule schedule, NThreading::TFuture<void> measurement_ended) {
    if (started_) {
        throw std::logic_error("reader " + std::to_string(id_) +
                               " was already started");
    }
    started_ = true;
    result_ = run(schedule, std::move(measurement_ended));
    return result_;
}

NThreading::TFuture<ReaderMeasurements> ReaderWorker::run(
    BenchmarkSchedule schedule, NThreading::TFuture<void> measurement_ended) {
    ReaderMeasurements measurements;
    ReaderPipeline pipeline(
        schedule, measurements.end_to_end, measurements.commit_ack);

    while (!pipeline.drained()) {
        auto event_ready = session_->WaitEvent();
        if (pipeline.reading()) {
            co_await NThreading::WaitAny(event_ready, measurement_ended);
            if (measurement_ended.IsReady()) {
                measurement_ended.GetValue();
                pipeline.finish_measurement();
            }
        } else {
            co_await std::move(event_ready);
        }

        if (pipeline.drained()) {
            break;
        }

        for (auto& event : session_->GetEvents(false)) {
            if (auto* data =
                    std::get_if<ReadEvent::TDataReceivedEvent>(&event)) {
                pipeline.on_data(*data);
                continue;
            }
            if (auto* acknowledgement =
                    std::get_if<ReadEvent::TCommitOffsetAcknowledgementEvent>(
                        &event)) {
                pipeline.on_commit_acknowledgement(*acknowledgement);
                continue;
            }
            if (auto* start =
                    std::get_if<ReadEvent::TStartPartitionSessionEvent>(
                        &event)) {
                pipeline.on_start_partition(*start);
                continue;
            }
            if (auto* stop = std::get_if<ReadEvent::TStopPartitionSessionEvent>(
                    &event)) {
                pipeline.on_stop_partition(*stop);
                continue;
            }
            if (auto* end =
                    std::get_if<ReadEvent::TEndPartitionSessionEvent>(&event)) {
                pipeline.on_end_partition(*end);
                continue;
            }
            if (std::holds_alternative<ReadEvent::TPartitionSessionStatusEvent>(
                    event)) {
                continue;
            }
            if (auto* closed =
                    std::get_if<ReadEvent::TPartitionSessionClosedEvent>(
                        &event)) {
                pipeline.on_partition_closed(*closed);
                continue;
            }

            const auto& closed =
                std::get<NYdb::NTopic::TSessionClosedEvent>(event);
            throw std::runtime_error("session closed unexpectedly: " +
                                     closed.DebugString());
        }
    }

    co_return std::move(measurements);
}

void ReaderWorker::shutdown() noexcept {
    if (stopped_) {
        return;
    }
    stopped_ = true;
    try {
        static_cast<void>(session_->Close(TDuration::Zero()));
    } catch (...) {
    }
    if (started_ && result_.Initialized()) {
        result_.Wait();
    }
}

}  // namespace sdk_compare
