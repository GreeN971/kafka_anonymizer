#include "schemas/http_log.capnp.h"
#include <algorithm>
#include <capnp/blob.h>
#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <capnp/common.h>
#include <capnp/serialize.h>
#include <capnp/c++.capnp.h>
#include <capnp/serialize-packed.h>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <kj/async.h>
#include <kj/common.h>
#include <kj/debug.h>
#include <kj/io.h>
#include <stdexcept>
#include <string>
#include <librdkafka/rdkafkacpp.h>
#include <csignal>
#include <iostream>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector> 
#include <kj/array.h>
#include <kj/common.h>
#include <Consumer.h>
#include <memory>
#include <thread>
#include <mutex>
#include <tuple>
#include <regex>
#include <algorithm>

static bool run = true;
void on_sigint(int) { run = false; }

ConsumerPtr CreateConsumer(RdKafka::Conf *conf)
{
    std::string error;
    auto *raw = RdKafka::KafkaConsumer::create(conf, error);
    if(!raw) 
        throw std::runtime_error("Failed to create consumer " + error);

    return ConsumerPtr(raw);
}

KafkaConfPtr ConfigureRole(int8_t role){
    auto raw = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if(!raw)
        throw std::runtime_error("Conf::create(CONF_GLOBAL) for consumer failed");
    KafkaConfPtr conf(raw);
    std::string globalErr; 
    
    auto set = [&](auto k, auto v){
        if(conf->set(k, v, globalErr) != RdKafka::Conf::CONF_OK)
            throw std::runtime_error(std::string("conf set '") + std::string(k) + "': " + globalErr);
    };

    //set("bootstrap.servers", "localhost:9092");
    // set("message.max.bytes", "10000");
    // set("topic.metadata.refresh.interval.ms", "-1"); //disabled
    // set("batch.num.messages", "10");

    if(role == 0) //consumer 
    {
        set("bootstrap.servers", "localhost:9092");
        set("message.max.bytes", "10000");
        set("topic.metadata.refresh.interval.ms", "-1"); //disabled
        //set("batch.num.messages", "10");

        set("enable.auto.commit", "false");
        set("group.id", "group.id");
        set("auto.offset.reset", "earliest");
        /*
            earliest means it will consume messeges from the begging but with latest it will consume
            only messages from when the consumer was alive, it means the time factor is present
        */
        return conf;
    }
    
    if(role == 1) //producer
    {
        set("bootstrap.servers", "localhost:9092");
        set("message.max.bytes", "10000");
        set("topic.metadata.refresh.interval.ms", "-1"); //disabled
        set("batch.num.messages", "10");
        
        set("compression.type", "lz4");
        set("compression.level", "6");
        set("enable.idempotence", "true");
        set("linger.ms", "61000"); //alias for queue.buffering.max.ms
        set("queue.buffering.max.messages", "200000");
        return conf;
    }

    throw std::runtime_error("Failed to choose role");
    return 0;
}

void Subscribe(RdKafka::KafkaConsumer *consumer, const std::vector<std::string> &topics)
{
    RdKafka::ErrorCode err = consumer->subscribe(topics);
    if(err) 
        std::runtime_error("Subscribe failed: " + RdKafka::err2str(err));
}

ProducerPtr CreateProducer(RdKafka::Conf *conf)
{
    std::string err;
    auto *raw = RdKafka::Producer::create(conf, err);
    if(!raw)
        throw std::runtime_error("Failed to create Producer" + err);

    return ProducerPtr(raw);
}

inline EditableLog DecodeToEditable(MessagePtr m)
{
    kj::ArrayPtr<const kj::byte> bytes(
        reinterpret_cast<const kj::byte*>(m->payload()), m->len());
    kj::ArrayInputStream is(bytes);
    auto keeper = std::make_shared<capnp::InputStreamMessageReader>(is);
    HttpLogRecord::Reader root = keeper->getRoot<HttpLogRecord>();
    
    auto rootCopy = std::make_shared<capnp::MallocMessageBuilder>();
    rootCopy->setRoot(root);
    HttpLogRecord::Builder log = rootCopy->getRoot<HttpLogRecord>();
    return {std::move(rootCopy), log};
}


void ValidateIPAdress(std::string_view ip)
{
    std::regex ipFormat(R"((\d{1,3}\.){3}\d{1,3})"); 
     /*
        /d digit format, \d{1,3} one to three digits in row ended by \. aka dot 
        {3} repeat three times
        plus the last one
    */
    std::match_results<std::string_view::const_iterator> it;
    if(std::regex_match(ip.begin(), ip.end(), it , ipFormat) == false)
        throw std::runtime_error("Invalid IP address format"); 
    std::cout << "Validated IP - ";
}

std::string MaskIP(std::string ip)
{
    ValidateIPAdress(ip);
   
    std::string replaceString = "X";
    static const std::regex regReplace(R"((\d{1,3}\.){3})");
    std::smatch match; 
    size_t secondDotPos;

    if(regex_search(ip, match, regReplace)){
        secondDotPos = match.position() + match.length();
    }
    ip = ip.substr(0, secondDotPos) + replaceString;
    return ip;
}

inline kj::Array<capnp::word> SerializeEdited(const EditableLog &log)
{
    return capnp::messageToFlatArray(*log.arena);
}

inline kj::ArrayPtr<const kj::byte> AsBytes(kj::Array<capnp::word>& words) 
{
  return words.asBytes();
}


int main()
{
    std::string errstr;
    std::string prodErr;
    Consumer con;
    // Create consumer
    //KafkaConfPtr conConf = ConfigureConsumer(con); //KafkaKey is where I find all metadata about my Kafka config
    //ConsumerPtr consumer = CreateConsumer(conConf.get());
    //Test new Config 
    KafkaConfPtr consumerConf = ConfigureRole(Role::ConsumerRole);
    ConsumerPtr consumer = CreateConsumer(consumerConf.get());
    //Create producer
    KafkaConfPtr producerConf = ConfigureRole(Role::ProducerRole);
    ProducerPtr producer = CreateProducer(producerConf.get());

    // Subscribe
    std::vector<std::string> topics{std::string(Topics::httpLog)};
    Subscribe(consumer.get(), topics);

    std::cout << "Consuming from topic \"" << Topics::httpLog << std::endl;

    const void *data;
    size_t len = 0;
    int i = 0;
    void *payload;
    //Sequence - Get Message, Decode Message, Put it into struct, Transform, Send via HTTP to clickhouse
    while (i != 10) {
        // poll() returns owned pointer; must delete it
        MessagePtr msg;
        msg.reset(consumer->consume(CONSUMER_TIMEOUT));
        switch (msg->err()) {
            case RdKafka::ERR_NO_ERROR:
                {
                    EditableLog editableLog = DecodeToEditable(std::move(msg));
                    std::string ip = std::string(editableLog.log.getRemoteAddr().cStr());   // copy out
                    std::string masked = MaskIP(ip);                                // returns std::string
                    editableLog.log.setRemoteAddr(masked); 
                    std::cout << masked;
                    // auto words = SerializeEdited(editableLog);
                    // auto bytes = AsBytes(words);
                    auto words = capnp::messageToFlatArray(*editableLog.arena);
                    auto bytes = words.asBytes();
                    
                    //TO DO TAKE LOOK AT THIS kj::EventLoop!!
                    /*
                    librdkafka is built to be scaled across threads – you can have multiple producers and consumers in different 
                    threads, and each is thread-safe (multiple threads can share a producer). In practice, it’s common to dedicate 
                    a thread for Kafka polling (especially for consumers). For example, in a multi-threaded edge server, one thread 
                    can handle network events and use Kafka producer asynchronously for logging, while another thread (or the same if 
                    integrated carefully) polls the producer for delivery reports and polls any consumer for incoming events. Many CDN 
                    systems embed Kafka I/O in existing loops (e.g., using the file descriptor provided by librdkafka for wakeups, to 
                    integrate with poll() or epoll). If using Cap’n Proto’s RPC/async framework (KJ event loop) alongside, you may 
                    coordinate the polling mechanisms (both librdkafka and Cap’n Proto can use pollable file descriptors, ensuring 
                    neither blocks the oth
                    */
                    
                    RdKafka::ErrorCode errCode = producer->produce(
                        "http_logs_test", 
                        RdKafka::Topic::PARTITION_UA, 
                        RdKafka::Producer::MSG_COPY, 
                        const_cast<void*>(static_cast<const void*>(bytes.begin())),
                        bytes.size(),
                        NULL,
                        0,
                        0,
                        NULL, 
                        NULL
                    );
                    if(errCode == RdKafka::ERR__QUEUE_FULL)
                        throw std::runtime_error("Queue is full");
                    
                    //message is returned and passed to Rd::kafka queue right away which is also managed by a different thread
                    //this thread will wait for one minute to send data via proxy. Reason for queue is that I can dump it all at once 
                    
                    //Assign partition to each message meaning I have to create partition here
                }
                break;
            case RdKafka::ERR__TIMED_OUT:
                // no message; continue polling
                break;
            case RdKafka::ERR__PARTITION_EOF:
                // reached end of partition (only if enable.partition.eof=true)
                break;
            default:
                std::cerr << "Consume error: " << msg->errstr() << "\n";
                break;
        }
        i++;
        producer->poll(1000);
    }
    producer->flush(300);
    std::cout << "Closing...\n";
    RdKafka::wait_destroyed(5000);
    return 0;
}

/*
//RdKafka::KafkaConsumer* consumer = RdKafka::KafkaConsumer::create(conf, errstr);
*/
