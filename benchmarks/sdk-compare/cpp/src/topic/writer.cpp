#include "writer.h"

#include <library/cpp/threading/future/core/coroutine_traits.h>
#include <library/cpp/threading/future/wait/wait.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "writer_worker.h"

namespace sdk_compare {
namespace {

std::size_t writer_count(const TopicWorkload& workload) {
    const auto partition_count =
        static_cast<std::size_t>(workload.partition_count);
    if (workload.writers_per_partition >
        std::numeric_limits<std::size_t>::max() / partition_count) {
        throw std::overflow_error("total writer count overflowed");
    }
    return partition_count * workload.writers_per_partition;
}

NThreading::TFuture<WriterMetrics> collect_writer_metrics(
    std::vector<NThreading::TFuture<WriterMeasurements>> results) {
    co_await NThreading::WaitAll(results);

    LatencyRecorder write_ack;
    for (const auto& result : results) {
        write_ack.merge(result.GetValue().write_ack);
    }

    co_return WriterMetrics{
        .write_ack = write_ack.summary(),
    };
}

}  // namespace

TopicWriters::TopicWriters(NYdb::NTopic::TTopicClient& topic_client,
                           const std::string& topic_path,
                           const TopicWorkload& workload) {
    writers_.reserve(writer_count(workload));
    std::size_t writer_id = 0;
    for (std::uint32_t partition_id = 0;
         partition_id < workload.partition_count;
         ++partition_id) {
        for (std::size_t writer_index = 0;
             writer_index < workload.writers_per_partition;
             ++writer_index) {
            writers_.push_back(std::make_unique<WriterWorker>(topic_client,
                                                              topic_path,
                                                              workload,
                                                              partition_id,
                                                              writer_index,
                                                              writer_id));
            ++writer_id;
        }
    }
}

TopicWriters::~TopicWriters() { shutdown(); }

NThreading::TFuture<WriterMetrics> TopicWriters::run(
    BenchmarkSchedule schedule, NThreading::TFuture<void> measurement_ended) {
    if (started_) {
        throw std::logic_error("writers were already started");
    }
    started_ = true;

    std::vector<NThreading::TFuture<WriterMeasurements>> results;
    results.reserve(writers_.size());
    for (auto& writer : writers_) {
        results.push_back(writer->start(schedule, measurement_ended));
    }

    result_ = collect_writer_metrics(std::move(results));
    return result_;
}

void TopicWriters::shutdown() noexcept {
    for (auto& writer : writers_) {
        writer->shutdown();
    }
    if (started_ && result_.Initialized()) {
        result_.Wait();
    }
}

}  // namespace sdk_compare
