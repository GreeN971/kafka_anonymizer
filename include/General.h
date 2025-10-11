#include <cstdint>
#include <cstdlib>
#include <librdkafka/rdkafkacpp.h>
#include <string>
#include <string_view>
#include <unistd.h>
#include <capnp/c++.capnp.h>
#include "schemas/http_log.capnp.h"
#include <memory>
#include <kj/common.h>
#include <kj/io.h>
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <type_traits>

#define CONSUMER_TIMEOUT 5000

using MessagePtr = std::unique_ptr<RdKafka::Message>;

namespace Topics{
    inline constexpr std::string_view HTTPLOG = "http_log";
    inline constexpr std::string_view HTTPLOGTEST = "http_logs_test";
}

struct EditableLog {
    std::shared_ptr<capnp::MallocMessageBuilder> arena; // owning arena
    HttpLogRecord::Builder log;                                    
};

void Subscribe(RdKafka::Consumer *consumer, std::vector<std::string> &topics);
