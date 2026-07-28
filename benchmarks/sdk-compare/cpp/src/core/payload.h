#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace sdk_compare {

std::string allocate_payload(std::size_t message_size_bytes);
void write_timestamp(std::string& payload, std::uint64_t sent_at_ns);
std::uint64_t read_timestamp(std::string_view payload);

}  // namespace sdk_compare
