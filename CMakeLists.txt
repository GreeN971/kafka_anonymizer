cmake_minimum_required(VERSION 3.28.1)
project(anonymizer VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE INTERNAL "")

find_package(RdKafka REQUIRED)
find_package(clickhouse-cpp REQUIRED)
find_package(CapnProto REQUIRED)
find_package(nlohmann_json REQUIRED)

capnp_generate_cpp(sources headers schemas/http_log.capnp)
add_library(http_log-capnp
    schemas/http_log.capnp
    ${sources}
    ${headers}
)

target_include_directories(http_log-capnp INTERFACE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(http_log-capnp PUBLIC
    capnproto::capnproto
)

add_executable(capnpClient
    src/capnpClient.cpp
)
target_include_directories(capnpClient PRIVATE include) #In case of not using delete

target_link_libraries(capnpClient PRIVATE 
    http_log-capnp
    RdKafka::rdkafka++
    nlohmann_json::nlohmann_json
)




