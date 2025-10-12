#include "schemas/http_log.capnp.h"
#include "General.h"
#include "ConfigBasedFactory.h"
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <iterator>
#include <kj/common.h>
#include <kj/units.h>
#include <librdkafka/rdkafkacpp.h>
#include <librdkafka/rdkafka.h>
#include <csignal>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <mutex>
#include <regex>
#include <future>
#include <queue>
#include <functional>

std::atomic<bool> g_shouldExit(false);
void onInterruptSignal(int) { g_shouldExit = true; }


void Subscribe(RdKafka::KafkaConsumer *consumer, const std::vector<std::string> &topics)
{
    RdKafka::ErrorCode err = consumer->subscribe(topics);
    if(err) 
        std::runtime_error("Subscribe failed: " + RdKafka::err2str(err));
}

inline EditableLog DecodeToEditable(RdKafka::Message *m)
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
        {3} repeat three times, plus the last one
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

void Thread1(RdKafka::Producer *producer, std::queue<MessagePtr> &queue, std::atomic<bool> &producerFinishSignal, 
            std::atomic<bool> &flushFinishSignal, std::mutex &qMutex,std::condition_variable &producerCond, RdKafka::Topic *topic)
{
    while(!producerFinishSignal.load() || !queue.empty())
        {
            MessagePtr message;
            {
                std::unique_lock<std::mutex> lk(qMutex); //When goes out of scope it automaticly lets go of the lock
                if(queue.empty() && !producerFinishSignal.load())
                    producerCond.wait(lk);
                if(queue.empty())
                    continue;
                message = std::move(queue.front());
                queue.pop(); //pops the nullptr which was left by the std::move
            }
            EditableLog editableLog = DecodeToEditable(message.get());
            std::string ip = std::string(editableLog.log.getRemoteAddr().cStr()); // copy out
            std::string masked = MaskIP(ip);
            editableLog.log.setRemoteAddr(masked); 
            
            std::cout << masked << " ";
            std::cout.flush();
            
            auto words = capnp::messageToFlatArray(*editableLog.arena);
            auto bytes = words.asBytes();

            RdKafka::ErrorCode errCode = producer->produce(
                topic->name(), 
                topic->PARTITION_UA, //automatic partitioning, but can be set to 1 as it is in the test enviroment
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
            {
                std::lock_guard<std::mutex> lk(qMutex);
                queue.push(std::move(message));
            }
        }
        flushFinishSignal.store(true);
}

int main()
{
    std::signal(SIGINT, onInterruptSignal);
    std::mutex unprocessedMessagesMutex;
    std::condition_variable producerCond;
    std::queue<MessagePtr> unprocessedMessages;
    std::atomic<bool> producerFinishSignal(false);
    std::atomic<bool> flushFinishSignal(false);
    /*
        Closing of each thread is done is concurent way. Meaning each thread has its own role. 
        Main thread is for the initial setup and then closing it all 
        T1 is for decoding, transforming and pushing transformed messages into producer queue. 
        T2 is just for flushing the messeges out of the producer queue 
        All of the work with signal callback which is called upon event. In this case abrupt closing of the program. 
            If abrupt closing happens first is T1 is notified. Currently processed messages which are inside unprocessed messages queue
            are pushed into producer queue. Then T1 joins the main thread and T2 flushes the producer queue and closes. 
            Thread closing order is arranged by atomics. 
    */
    
    ConfigBasedFactory configBasedFactory("config/config.json");
    //Create consumer
    ConsumerPtr consumer = configBasedFactory.CreateConsumer("consumer");

    //Create producer
    ProducerPtr producer = configBasedFactory.CreateProducer("producer");

    //Create topic
    TopicPtr topic = configBasedFactory.CreateTopic("topic", Topics::HTTPLOGTEST, producer.get());

    auto t1 = std::thread(&Thread1, producer.get(), std::ref(unprocessedMessages), std::ref(producerFinishSignal), 
            std::ref(flushFinishSignal), std::ref(unprocessedMessagesMutex), std::ref(producerCond), topic.get());    

    /*
        async has to be stored since it returns future which eventualy get destroyed. If it was not stored main thread would wait
        for this one to finish. 
    */
    auto t2 = std::async([&producer, &flushFinishSignal](){ 
        while(!flushFinishSignal.load())
            producer->flush(50); 
    });

    std::vector<std::string> topics{std::string(Topics::HTTPLOG)};
    Subscribe(consumer.get(), topics);

    std::cout << "Consuming from topic \"" << Topics::HTTPLOG << std::endl;

    //Sequence - Get Message, Decode Message, Put it into struct, Transform, Send via HTTP to clickhouse
    while (!g_shouldExit) {
        // poll() returns owned pointer; must delete it
        MessagePtr msg;
        msg.reset(consumer->consume(CONSUMER_TIMEOUT));
        switch (msg->err()) {
            case RdKafka::ERR_NO_ERROR:
                {
                    std::lock_guard<std::mutex> lk(unprocessedMessagesMutex);
                    unprocessedMessages.push(std::move(msg));
                    producerCond.notify_one();
                }
                break;
            case RdKafka::ERR__TIMED_OUT:
                break;
            default:
                std::cerr << "Consume error: " << msg->errstr() << "\n";
                break;
        }
    }

    {
        std::lock_guard<std::mutex> lk(unprocessedMessagesMutex);
        producerFinishSignal.store(true);
        producerCond.notify_one();
    }

    t1.join();
    t2.get();
    std::cout << " Closing...\n";
    RdKafka::wait_destroyed(5000);
    return 0;
}
