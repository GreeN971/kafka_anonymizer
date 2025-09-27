#include <capnp/common.h>
#include <capnp/message.h>
#include <cstddef>
#include <iostream>
#include <kj/async.h>
#include <librdkafka/rdkafkacpp.h>
#include <librdkafka/rdkafka.h>
#include <capnp/ez-rpc.h>

int main(int argc, char **argv) {
  RdKafka::BrokerMetadata *broker; 
  broker->host();
  capnp::ByteCount count;
  capnp::MallocMessageBuilder message;
  
  return 0;
}
/**


 */