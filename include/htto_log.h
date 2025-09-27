#include <cstdint>
#include <string>

struct HttpLog{
    int64_t timestampEpochMilli;
    int64_t resourceId;
    int64_t bytesSent;
    int64_t requestTimeMilli;
    int64_t responseStatus;
    std::string cacheStatus;
    std::string method ;
    std::string remoteAddr; //IP format
    std::string url;

    struct KeyInfo{
        int32_t partition;
        int64_t offset;
        size_t len;
    }key;
};

struct RawMsg{
    std::string topic;
    int32_t partition;
    int64_t offset;
    std::string payload;
};