#pragma once

#include "core/result.h"
#include "core/scenario.h"

namespace sdk_compare {

BenchmarkResult run_topic_benchmark(const Scenario& scenario,
                                    const TopicWorkload& workload);

}  // namespace sdk_compare
