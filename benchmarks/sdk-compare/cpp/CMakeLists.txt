cmake_minimum_required(VERSION 3.25)

project(ydb-sdk-compare LANGUAGES C CXX)

include(FetchContent)

set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(HDR_HISTOGRAM_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(HDR_LOG_REQUIRED OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    hdr_histogram
    URL https://github.com/HdrHistogram/HdrHistogram_c/archive/8dcce8f68512fca460b171bccc3a5afce0048779.tar.gz
    URL_HASH SHA256=295d26edcb164134a6a40a4b0db4fadd25673aa7abc39471b882cbfb5e31d999
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

FetchContent_MakeAvailable(hdr_histogram)

set(SDK_COMPARE_YDB_SDK_SOURCE "" CACHE PATH "Path to a YDB C++ SDK source checkout")

if(NOT SDK_COMPARE_YDB_SDK_SOURCE)
    message(FATAL_ERROR "SDK_COMPARE_YDB_SDK_SOURCE is required")
endif()

add_subdirectory(
    "${SDK_COMPARE_YDB_SDK_SOURCE}"
    "${CMAKE_CURRENT_BINARY_DIR}/ydb-cpp-sdk"
)

add_executable(ydb-sdk-compare
    src/main.cpp
    src/core/metrics.cpp
    src/core/payload.cpp
    src/core/result.cpp
    src/core/scenario.cpp
    src/core/schedule.cpp
    src/topic/benchmark.cpp
    src/topic/fixture.cpp
    src/topic/reader.cpp
    src/topic/reader_worker.cpp
    src/topic/writer.cpp
    src/topic/writer_worker.cpp
)

target_include_directories(ydb-sdk-compare PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_features(ydb-sdk-compare PRIVATE cxx_std_20)
set_target_properties(ydb-sdk-compare PROPERTIES CXX_EXTENSIONS OFF)

set(SDK_COMPARE_SANITIZERS "" CACHE STRING "Sanitizers for the benchmark target")

target_compile_options(ydb-sdk-compare PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    # Required by headers from the pinned SDK when compiled with Clang 21.
    -Wno-deprecated-literal-operator
    -Wno-logical-op-parentheses
    -Werror
)

if(SDK_COMPARE_SANITIZERS)
    target_compile_options(ydb-sdk-compare PRIVATE
        "-fsanitize=${SDK_COMPARE_SANITIZERS}"
        -fno-omit-frame-pointer
    )
    target_link_options(ydb-sdk-compare PRIVATE
        "-fsanitize=${SDK_COMPARE_SANITIZERS}"
    )
endif()

target_link_libraries(ydb-sdk-compare PRIVATE
    YDB-CPP-SDK::Helpers
    YDB-CPP-SDK::Topic
    hdr_histogram_static
)
