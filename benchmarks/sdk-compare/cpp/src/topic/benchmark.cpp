#include "benchmark.h"

#include <library/cpp/threading/future/future.h>

#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include "core/schedule.h"
#include "fixture.h"
#include "reader.h"
#include "writer.h"

namespace sdk_compare {
namespace {

template <typename Metrics>
Metrics wait_for_metrics(const NThreading::TFuture<Metrics>& result,
                         BenchmarkSchedule::TimePoint deadline,
                         std::string_view workers) {
    if (!result.Wait(remaining_timeout(deadline))) {
        throw std::runtime_error(std::string(workers) + " drain timed out");
    }
    return result.GetValue();
}

TopicMetrics topic_metrics(const WriterMetrics& writers,
                           const ReaderMetrics& readers,
                           const Scenario& scenario,
                           const TopicWorkload& workload) {
    const auto measurement_seconds =
        static_cast<double>(scenario.execution.measurement_seconds);
    const auto message_size_bytes =
        static_cast<double>(workload.message_size_bytes);
    const auto write_messages_per_second =
        static_cast<double>(writers.write_ack.count) / measurement_seconds;
    const auto read_messages_per_second =
        static_cast<double>(readers.end_to_end.count) / measurement_seconds;

    return TopicMetrics{
        .write_ack = writers.write_ack,
        .end_to_end = readers.end_to_end,
        .commit_ack = readers.commit_ack,
        .write_messages_per_second = write_messages_per_second,
        .write_bytes_per_second =
            write_messages_per_second * message_size_bytes,
        .read_messages_per_second = read_messages_per_second,
        .read_bytes_per_second = read_messages_per_second * message_size_bytes,
    };
}

}  // namespace

BenchmarkResult run_topic_benchmark(const Scenario& scenario,
                                    const TopicWorkload& workload) {
    TopicFixture fixture(scenario.execution, workload);
    TopicWriters writers(
        fixture.topic_client(), fixture.topic_path(), workload);
    TopicReaders readers(
        fixture.topic_client(), fixture.topic_path(), workload);

    const auto schedule = BenchmarkSchedule::start(scenario.execution);
    auto measurement_ended = NThreading::NewPromise();
    const auto reader_result =
        readers.run(schedule, measurement_ended.GetFuture());
    const auto writer_result =
        writers.run(schedule, measurement_ended.GetFuture());

    std::this_thread::sleep_until(schedule.measurement_end());
    measurement_ended.SetValue();

    const auto reader_metrics = wait_for_metrics(
        reader_result, schedule.completion_deadline(), "readers");
    const auto writer_metrics = wait_for_metrics(
        writer_result, schedule.completion_deadline(), "writers");

    return BenchmarkResult{
        .scenario = scenario,
        .implementation = cpp_implementation(),
        .metrics =
            topic_metrics(writer_metrics, reader_metrics, scenario, workload),
    };
}

}  // namespace sdk_compare
