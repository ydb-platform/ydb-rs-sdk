#include "payload.h"

#include <stdexcept>

#include "scenario.h"

namespace sdk_compare {
namespace {

inline constexpr unsigned char fill_byte = 0xA5;

void require_header(std::size_t size, std::string_view subject) {
    if (size < topic_payload_header_size_bytes) {
        throw std::runtime_error(
            std::string(subject) + " is shorter than the " +
            std::to_string(topic_payload_header_size_bytes) + "-byte header");
    }
}

}  // namespace

std::string allocate_payload(std::size_t message_size_bytes) {
    require_header(message_size_bytes, "message size");
    std::string payload(message_size_bytes, static_cast<char>(fill_byte));
    payload.replace(0,
                    topic_payload_header_size_bytes,
                    topic_payload_header_size_bytes,
                    '\0');
    return payload;
}

void write_timestamp(std::string& payload, std::uint64_t sent_at_ns) {
    require_header(payload.size(), "payload");
    for (std::size_t index = 0; index < topic_payload_header_size_bytes;
         ++index) {
        payload[index] =
            static_cast<char>((sent_at_ns >> (index * 8U)) & 0xFFU);
    }
}

std::uint64_t read_timestamp(std::string_view payload) {
    require_header(payload.size(), "payload");
    std::uint64_t sent_at_ns = 0;
    for (std::size_t index = 0; index < topic_payload_header_size_bytes;
         ++index) {
        const auto byte = static_cast<unsigned char>(payload[index]);
        sent_at_ns |= static_cast<std::uint64_t>(byte) << (index * 8U);
    }
    return sent_at_ns;
}

}  // namespace sdk_compare
