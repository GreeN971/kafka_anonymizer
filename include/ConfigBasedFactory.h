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

class ConfigBasedFactory 
{
public: 
    ConfigBasedFactory();
    ConfigBasedFactory(std::string_view path)
    {
        m_data = nlohmann::json::parse(std::ifstream(path.data()));
    }
    /*
        1) Separate core logic of Configure to self contained method that will create conf (move it to private block)
        2) Create 3 methods -> CreateTopic, CreateProducer, CreateConsumer
            | - each methods will accept arguments specific to topic/producer/consumer creation + key (you already know which ConfType for which object to pass)
    */

    KafkaConfPtr CreateConf(RdKafka::Conf::ConfType confType, std::string_view key)
    {
        auto raw = RdKafka::Conf::create(confType);
        if(!raw)
            std::runtime_error("Conf::Create failed");
        return Configure(key, raw);
    }

    ConsumerPtr CreateConsumer(std::string_view key)
    {
        std::string err;
        auto *raw = RdKafka::KafkaConsumer::create(CreateConf(RdKafka::Conf::CONF_GLOBAL,key).get(), err);
        if(!raw)
            throw std::runtime_error("Failed to create consumer " + err);
        return ConsumerPtr(raw);
    }
  
    ProducerPtr CreateProducer(std::string_view key)
    {
        std::string err;
        auto *raw = RdKafka::Producer::create(CreateConf(RdKafka::Conf::CONF_GLOBAL, key).get(), err);
        if(!raw)
            throw std::runtime_error("Failed to create consumer " + err);
        return ProducerPtr(raw);
    }

    TopicPtr CreateTopic(std::string_view key, std::string_view topicName, RdKafka::Handle *handle)
    {
        std::string err;
        auto *raw = RdKafka::Topic::create(handle, topicName.data() ,CreateConf(RdKafka::Conf::CONF_TOPIC, key).get(), err);
        if(!raw)
            throw std::runtime_error("Failed to create consumer " + err);
        return TopicPtr(raw);
    }

    KafkaConfPtr Configure(std::string_view key, RdKafka::Conf *conf)
    { 
        std::string globalErr; 
        auto set = [&](auto k, auto v){
            if(conf->set(k, v, globalErr) != RdKafka::Conf::CONF_OK)
                throw std::runtime_error(std::string("conf set '") + std::string(k) + "': " + globalErr);
        };

        for(const nlohmann::json &property : m_data[key])
        {
            if(conf->set(property["propertyName"].get_ref< const std::string&>(),
                property["value"].get_ref<const std::string&>(), globalErr) != RdKafka::Conf::CONF_OK)
                throw std::runtime_error(std::string("conf set ") + 
                    property["propertyName"].get_ref<const std::string&>() + "': " + globalErr);
        }
        return KafkaConfPtr(conf);
    }

protected: 
    const nlohmann::json &GetData() {return m_data;}
private:
    nlohmann::json m_data;
};