#include "reader.h"

#include <library/cpp/threading/future/core/coroutine_traits.h>
#include <library/cpp/threading/future/wait/wait.h>

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "reader_worker.h"

namespace sdk_compare {
namespace {

NThreading::TFuture<ReaderMetrics> collect_reader_metrics(
    std::vector<NThreading::TFuture<ReaderMeasurements>> results) {
    co_await NThreading::WaitAll(results);

    LatencyRecorder end_to_end;
    LatencyRecorder commit_ack;
    for (const auto& result : results) {
        const auto& measurements = result.GetValue();
        end_to_end.merge(measurements.end_to_end);
        commit_ack.merge(measurements.commit_ack);
    }

    co_return ReaderMetrics{
        .end_to_end = end_to_end.summary(),
        .commit_ack = commit_ack.summary(),
    };
}

}  // namespace

TopicReaders::TopicReaders(NYdb::NTopic::TTopicClient& topic_client,
                           const std::string& topic_path,
                           const TopicWorkload& workload) {
    readers_.reserve(workload.reader_count);
    for (std::size_t reader_id = 0; reader_id < workload.reader_count;
         ++reader_id) {
        readers_.push_back(std::make_unique<ReaderWorker>(
            topic_client, topic_path, workload, reader_id));
    }
}

TopicReaders::~TopicReaders() { shutdown(); }

NThreading::TFuture<ReaderMetrics> TopicReaders::run(
    BenchmarkSchedule schedule, NThreading::TFuture<void> measurement_ended) {
    if (started_) {
        throw std::logic_error("readers were already started");
    }
    started_ = true;

    std::vector<NThreading::TFuture<ReaderMeasurements>> results;
    results.reserve(readers_.size());
    for (auto& reader : readers_) {
        results.push_back(reader->start(schedule, measurement_ended));
    }

    result_ = collect_reader_metrics(std::move(results));
    return result_;
}

void TopicReaders::shutdown() noexcept {
    for (auto& reader : readers_) {
        reader->shutdown();
    }
    if (started_ && result_.Initialized()) {
        result_.Wait();
    }
}

}  // namespace sdk_compare
