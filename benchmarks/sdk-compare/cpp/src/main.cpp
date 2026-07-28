#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>

#include "core/scenario.h"
#include "topic/benchmark.h"

namespace {

std::filesystem::path scenario_path(int argc, char* argv[]) {
    if (argc < 2) {
        throw std::runtime_error("usage: " + std::string(argv[0]) +
                                 " <scenario.json>");
    }
    if (argc > 2) {
        throw std::runtime_error("expected exactly one scenario file argument");
    }
    return argv[1];
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        const auto scenario =
            sdk_compare::load_scenario(scenario_path(argc, argv));
        const auto& workload =
            std::get<sdk_compare::TopicWorkload>(scenario.workload);
        const auto result =
            sdk_compare::run_topic_benchmark(scenario, workload);
        std::cout << nlohmann::json(result).dump(2) << '\n';
        if (!std::cout) {
            throw std::runtime_error("failed to write result JSON");
        }
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
