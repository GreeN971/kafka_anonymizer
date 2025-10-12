#include "ConfigBasedFactory.h"

ConfigBasedFactory::ConfigBasedFactory(std::string_view path)
{
    m_data = nlohmann::json::parse(std::ifstream(path.data()));
}

KafkaConfPtr ConfigBasedFactory::CreateConf(RdKafka::Conf::ConfType confType, std::string_view key)
{
    auto raw = RdKafka::Conf::create(confType);
    if(!raw)
        std::runtime_error("Conf::Create failed");
    return Configure(key, raw);
}

ConsumerPtr ConfigBasedFactory::CreateConsumer(std::string_view key)
{
    std::string err;
    auto *raw = RdKafka::KafkaConsumer::create(CreateConf(RdKafka::Conf::CONF_GLOBAL,key).get(), err);
    if(!raw)
        throw std::runtime_error("Failed to create consumer " + err);
    return ConsumerPtr(raw);
}

ProducerPtr ConfigBasedFactory::CreateProducer(std::string_view key)
{
    std::string err;
    auto *raw = RdKafka::Producer::create(CreateConf(RdKafka::Conf::CONF_GLOBAL, key).get(), err);
    if(!raw)
        throw std::runtime_error("Failed to create consumer " + err);
    return ProducerPtr(raw);
}

TopicPtr ConfigBasedFactory::CreateTopic(std::string_view key, std::string_view topicName, RdKafka::Handle *handle)
{
    std::string err;
    auto *raw = RdKafka::Topic::create(handle, topicName.data() ,CreateConf(RdKafka::Conf::CONF_TOPIC, key).get(), err);
    if(!raw)
        throw std::runtime_error("Failed to create consumer " + err);
    return TopicPtr(raw);
}

KafkaConfPtr ConfigBasedFactory::Configure(std::string_view key, RdKafka::Conf *conf)
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