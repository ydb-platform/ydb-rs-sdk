#include "writer_worker.h"

#include <library/cpp/threading/future/core/coroutine_traits.h>
#include <library/cpp/threading/future/wait/wait.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "core/payload.h"

namespace sdk_compare {
namespace {

using WriteSession = NYdb::NTopic::IWriteSession;
using WriteEvent = NYdb::NTopic::TWriteSessionEvent;
using ContinuationToken = NYdb::NTopic::TContinuationToken;

struct PendingWrite {
    BenchmarkSchedule::TimePoint started;
    bool measured;
};

NYdb::NTopic::TWriteSessionSettings writer_settings(
    const std::string& topic_path,
    const TopicWorkload& workload,
    std::uint32_t partition_id,
    std::size_t writer_index) {
    const auto producer_id = "sdk-compare-writer-" +
                             std::to_string(partition_id) + '-' +
                             std::to_string(writer_index);
    auto settings = NYdb::NTopic::TWriteSessionSettings();
    settings.Path(topic_path)
        .ProducerId(producer_id)
        .PartitionId(partition_id)
        .DirectWriteToPartition(false)
        .Codec(NYdb::NTopic::ECodec::RAW)
        .BatchFlushMessageCount(
            static_cast<std::uint32_t>(workload.write_batch_max_messages))
        .BatchFlushInterval(
            TDuration::MilliSeconds(workload.write_batch_max_delay_ms));
    return settings;
}

class WriterPipeline {
public:
    WriterPipeline(WriteSession& session,
                   BenchmarkSchedule schedule,
                   std::size_t message_size_bytes,
                   std::size_t max_in_flight,
                   LatencyRecorder& write_ack)
        : session_(session)
        , schedule_(schedule)
        , message_size_bytes_(message_size_bytes)
        , max_in_flight_(max_in_flight)
        , write_ack_(write_ack) {
        pending_writes_.reserve(max_in_flight);
    }

    void on_ready(ContinuationToken token) {
        if (!submissions_open_) {
            return;
        }
        if (held_token_) {
            throw std::runtime_error(
                "received a continuation token while another token is pending");
        }
        held_token_ = std::move(token);
        submit_if_possible();
    }

    void on_acknowledgements(const WriteEvent::TAcksEvent& event) {
        const auto acknowledged_at = BenchmarkSchedule::Clock::now();
        for (const auto& acknowledgement : event.Acks) {
            acknowledge(acknowledgement, acknowledged_at);
        }
        submit_if_possible();
    }

    bool drained() const {
        return !submissions_open_ && pending_writes_.empty();
    }

    bool submitting() const { return submissions_open_; }

    void finish_measurement() {
        submissions_open_ = false;
        held_token_.reset();
    }

private:
    void acknowledge(const WriteEvent::TWriteAck& acknowledgement,
                     BenchmarkSchedule::TimePoint acknowledged_at) {
        const auto pending = pending_writes_.find(acknowledgement.SeqNo);
        if (pending == pending_writes_.end()) {
            throw std::runtime_error(
                "received acknowledgement for unknown sequence number " +
                std::to_string(acknowledgement.SeqNo));
        }
        if (acknowledgement.State != WriteEvent::TWriteAck::EES_WRITTEN &&
            acknowledgement.State !=
                WriteEvent::TWriteAck::EES_ALREADY_WRITTEN) {
            throw std::runtime_error("sequence number " +
                                     std::to_string(acknowledgement.SeqNo) +
                                     " was not persisted");
        }
        if (pending->second.measured) {
            write_ack_.record(acknowledged_at - pending->second.started);
        }
        pending_writes_.erase(pending);
    }

    void submit_if_possible() {
        if (!submissions_open_ || !held_token_ ||
            pending_writes_.size() >= max_in_flight_) {
            return;
        }

        auto token = std::exchange(held_token_, std::nullopt);
        auto payload = allocate_payload(message_size_bytes_);
        const auto started = BenchmarkSchedule::Clock::now();
        if (started >= schedule_.measurement_end()) {
            submissions_open_ = false;
            return;
        }
        write_timestamp(payload, schedule_.ns_at(started));

        // In automatic sequence mode, the pinned SDK exposes session-local
        // acknowledgement IDs in submission order, starting at one.
        const auto sequence_number = next_acknowledgement_sequence_number_++;
        pending_writes_.emplace(
            sequence_number,
            PendingWrite{
                .started = started,
                .measured = schedule_.is_measurement_instant(started),
            });
        session_.Write(std::move(*token), NYdb::NTopic::TWriteMessage(payload));
    }

    WriteSession& session_;
    BenchmarkSchedule schedule_;
    const std::size_t message_size_bytes_;
    const std::size_t max_in_flight_;
    LatencyRecorder& write_ack_;
    std::optional<ContinuationToken> held_token_;
    std::unordered_map<std::uint64_t, PendingWrite> pending_writes_;
    std::uint64_t next_acknowledgement_sequence_number_ = 1;
    bool submissions_open_ = true;
};

ContinuationToken wait_for_initial_token(WriteSession& session,
                                         std::size_t writer_id) {
    auto event = session.GetEvent(true);
    auto* ready =
        event ? std::get_if<WriteEvent::TReadyToAcceptEvent>(&*event) : nullptr;
    if (!ready) {
        throw std::runtime_error("writer " + std::to_string(writer_id) +
                                 " did not become ready");
    }
    return std::move(ready->ContinuationToken);
}

}  // namespace

WriterWorker::WriterWorker(NYdb::NTopic::TTopicClient& topic_client,
                           const std::string& topic_path,
                           const TopicWorkload& workload,
                           std::uint32_t partition_id,
                           std::size_t writer_index,
                           std::size_t writer_id)
    : id_(writer_id)
    , message_size_bytes_(workload.message_size_bytes)
    , max_in_flight_(workload.max_in_flight_per_writer)
    , session_(topic_client.CreateWriteSession(
          writer_settings(topic_path, workload, partition_id, writer_index))) {
    if (!session_) {
        throw std::runtime_error(
            "failed to create writer " + std::to_string(writer_index) +
            " for partition " + std::to_string(partition_id));
    }
    initial_token_ = wait_for_initial_token(*session_, id_);
}

WriterWorker::~WriterWorker() { shutdown(); }

NThreading::TFuture<WriterMeasurements> WriterWorker::start(
    BenchmarkSchedule schedule, NThreading::TFuture<void> measurement_ended) {
    if (started_) {
        throw std::logic_error("writer " + std::to_string(id_) +
                               " was already started");
    }
    started_ = true;

    auto initial_token = std::exchange(initial_token_, std::nullopt).value();
    result_ =
        run(schedule, std::move(measurement_ended), std::move(initial_token));
    return result_;
}

NThreading::TFuture<WriterMeasurements> WriterWorker::run(
    BenchmarkSchedule schedule,
    NThreading::TFuture<void> measurement_ended,
    ContinuationToken initial_token) {
    WriterMeasurements measurements;
    WriterPipeline pipeline(*session_,
                            schedule,
                            message_size_bytes_,
                            max_in_flight_,
                            measurements.write_ack);
    pipeline.on_ready(std::move(initial_token));

    while (!pipeline.drained()) {
        auto event_ready = session_->WaitEvent();
        if (pipeline.submitting()) {
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
            if (auto* ready_event =
                    std::get_if<WriteEvent::TReadyToAcceptEvent>(&event)) {
                pipeline.on_ready(std::move(ready_event->ContinuationToken));
                continue;
            }
            if (auto* acknowledgements =
                    std::get_if<WriteEvent::TAcksEvent>(&event)) {
                pipeline.on_acknowledgements(*acknowledgements);
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

void WriterWorker::shutdown() noexcept {
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
