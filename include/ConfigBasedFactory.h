#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <string>
#include <string_view>
#include <memory>

struct ConsumerDeleter {
    void operator()(RdKafka::KafkaConsumer *ptr) const {
        if (ptr) {
            ptr->close();
            delete ptr;
        }
    }
};

using KafkaConfPtr = std::unique_ptr<RdKafka::Conf>;
using ConsumerPtr = std::unique_ptr<RdKafka::KafkaConsumer, ConsumerDeleter>;
using ProducerPtr = std::unique_ptr<RdKafka::Producer>;
using TopicPtr = std::unique_ptr<RdKafka::Topic>;

class ConfigBasedFactory final
{
public: 
    ConfigBasedFactory();
    ConfigBasedFactory(std::string_view path);

    KafkaConfPtr CreateConf(RdKafka::Conf::ConfType confType, std::string_view key);

    ConsumerPtr CreateConsumer(std::string_view key);
  
    ProducerPtr CreateProducer(std::string_view key);

    TopicPtr CreateTopic(std::string_view key, std::string_view topicName, RdKafka::Handle *handle);

    KafkaConfPtr Configure(std::string_view key, RdKafka::Conf *conf);

private:
    nlohmann::json m_data;
};