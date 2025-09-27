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
#include <htto_log.h>
#include <type_traits>

#define CONSUMER_TIMEOUT 5000

enum Role {ProducerRole = 1, ConsumerRole = 0};

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


namespace Topics{
    inline constexpr std::string_view httpLog = "http_log";
}


struct ConsumerResutl{
    RdKafka::KafkaConsumer *consumer;
    std::string error;
};

struct EditableLog {
    std::shared_ptr<capnp::MallocMessageBuilder> arena; // owning arena
    HttpLogRecord::Builder log;                                    
};

class Consumer{
public:
    KafkaConfPtr ConfigureRole(Role &role);
    RdKafka::Conf ConfigureConsumer(Consumer &con);
    RdKafka::Consumer CreateConsumer(RdKafka::Conf *conf);
    void Subscribe(RdKafka::Consumer consumer, std::vector<std::string> &topics);

    //DEFINE_GETTER(Broker, brokers;);

    const std::string &GetCapture() const noexcept{
        return capture;
    }


protected:
    std::string capture;

private:
    
};


/*

protected:
    const std::string brokers = "localhost:9092";
    const std::string group_id = "demo-group";
    const std::string topic ="http_log";

    const std::string &GetTopic() const noexcept{
        return topic;
    }
    
    const std::string &GetBroker() const noexcept{
        return brokers;
    }

    const std::string &GetGroupID() const noexcept{
        return group_id;
    }
*/