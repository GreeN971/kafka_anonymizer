#include <cstdint>
#include <cstdlib>
#include <librdkafka/rdkafkacpp.h>
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

struct ConsumerDeleter {
    void operator()(RdKafka::KafkaConsumer *ptr) const {
        if (ptr) {
            ptr->close();
            delete ptr;
        }
    }
};

using KafkaConfPtr = std::unique_ptr<RdKafka::Conf>;
using MessagePtr = std::unique_ptr<RdKafka::Message>;
using ConsumerPtr = std::unique_ptr<RdKafka::KafkaConsumer, ConsumerDeleter>;
using ProducerPtr = std::unique_ptr<RdKafka::Producer>;
using TopicPtr = std::unique_ptr<RdKafka::Topic>;

namespace Topics{
    inline constexpr std::string_view HTTPLOG = "http_log";
    inline constexpr std::string_view HTTPLOGTEST = "http_logs_test";
}

struct EditableLog {
    std::shared_ptr<capnp::MallocMessageBuilder> arena; // owning arena
    HttpLogRecord::Builder log;                                    
};

KafkaConfPtr Configure(RdKafka::Conf::ConfType);
ConsumerPtr CreateConsumer(RdKafka::Conf *conf);
void Subscribe(RdKafka::Consumer *consumer, std::vector<std::string> &topics);

