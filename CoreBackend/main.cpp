#include "message.hh"
#include "avro/Encoder.hh"
#include "avro/Stream.hh"
#include "avro/ValidSchema.hh"
#include <curl/curl.h>
#include <random>
#include <chrono>
#include <vector>
#include <string>
#include "avro/Compiler.hh"
#include <jsoncpp/json/json.h>
size_t writeCallback(char *ptr, size_t size, size_t nmemb, std::string *data)
{
  size_t totalSize = size * nmemb;
  data->append(ptr, totalSize);
  return totalSize;
}

std::vector<std::pair<std::string, std::string>> schemaFields = {
    {"type", "int"},
    {"timestamp", "long"},
    {"metadata", "string"},
    {"payload", "string"}};

int size = 0;

void sendMessage(const std::string &encodedData)
{
  CURL *curl = curl_easy_init();
  if (curl)
  {
    size += encodedData.size();
    std::cout << encodedData << '\n';
    std::string responseData;
    std::string url = "http://localhost:8080/api/messages";
    struct curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/octet-stream");
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, encodedData.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, encodedData.size());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseData);
    CURLcode res = curl_easy_perform(curl);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
}

int jsonsize = 0;

void sendMessageJSON(const std::string& jsonData) {
    CURL* curl = curl_easy_init();
    if (curl) {
      jsonsize += jsonData.size();
        std::string responseData;
        std::string url = "http://localhost:8080/api/messages";
        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonData.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, jsonData.size());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseData);
        CURLcode res = curl_easy_perform(curl);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    }
}



std::string generateRandomString(int length) {
    static const char alphabet[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> distribution(0, sizeof(alphabet) - 2);

    std::string randomString;
    randomString.reserve(length);
    for (int i = 0; i < length; ++i) {
        randomString += alphabet[distribution(rng)];
    }
    return randomString;
}

int main()
{
  // // // Create an instance of the Avro message struct
  // //  message::Message avroMessage;
  // // avroMessage.type = 1;
  // // avroMessage.timestamp = 1621512000;  // Replace with your desired timestamp
  // // avroMessage.metadata = "Some metadata";
  // // avroMessage.payload = "Payload data";

  // // // Encode the Avro message
  // // avro::OutputStreamPtr out = avro::memoryOutputStream();
  // // avro::EncoderPtr encoder = avro::binaryEncoder();
  // // encoder->init(*out);
  // // avro::encode(*encoder, avroMessage);
  // // encoder->flush();

  // // // Get the encoded data as a string
  // // std::shared_ptr<std::vector<uint8_t>> buffer = avro::snapshot(*out);
  // // std::vector<uint8_t> rawData = *buffer;

  // //   std::string url = "http://localhost:8080/api/messages";
  // //   send_post_request(url, rawData);

  // // // Send the encoded data to your Golang app via HTTP request
  // // // (You'll need to use a suitable HTTP client library)

  // Create a random number generator
  std::random_device rd;
  std::mt19937 rng(rd());
  std::uniform_int_distribution<int> typeDist(1, 100);
  std::uniform_int_distribution<int64_t> timestampDist(0, std::numeric_limits<int64_t>::max());
  std::uniform_int_distribution<int> metadataLengthDist(10, 100);
  std::uniform_int_distribution<int> payloadLengthDist(10000, 20000);

  // Create an Avro message schema
  avro::ValidSchema messageSchema = avro::compileJsonSchemaFromString(R"(
    {
      "type": "record",
      "name": "Message",
      "fields": [
        { "name": "type", "type": "int" },
        { "name": "timestamp", "type": "long" },
        { "name": "metadata", "type": "string" },
        { "name": "payload", "type": "string" }
      ]
    }
  )");

  // Encode and send 10,000 random Avro messages

  auto val1 = typeDist(rng);
  auto val2 = timestampDist(rng);
  auto val3 = generateRandomString(metadataLengthDist(rng));
  auto val4 = generateRandomString(payloadLengthDist(rng)); 
  // Start the benchmark timer
  auto startTimeAvro = std::chrono::steady_clock::now();
  const int numMessages = 100000;
  for (int i = 0; i < numMessages; ++i)
  {
    // Create a new Avro message
    message::Message avroMessage;
    avroMessage.type = val1;
    avroMessage.timestamp = val2; 
    avroMessage.metadata =  val3; // Random metadata string
    avroMessage.payload =  val4;

    // Encode the Avro message
    avro::OutputStreamPtr out = avro::memoryOutputStream();
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::encode(*encoder, avroMessage);
    encoder->flush();

    // Get the encoded data as a string
    std::shared_ptr<std::vector<uint8_t>> buffer = avro::snapshot(*out);
    std::vector<uint8_t> rawData = *buffer;
    std::string encodedData(rawData.begin(), rawData.end());

    std::string url = "http://localhost:8080/api/messages";

    
    sendMessage(encodedData);
    break;
  }

  auto endTimeAvro = std::chrono::steady_clock::now();

  auto startTimeJson = std::chrono::steady_clock::now();
  for (int i = 0; i < numMessages; ++i) {
  break;
    Json::Value jsonData;
    jsonData["type"] = val1;
    jsonData["timestamp"] = val2;
    jsonData["metadata"] = val3;
    jsonData["payload"] = val4;

                            // Convert the JSON payload to a string
                            Json::StreamWriterBuilder writer;
    std::string jsonString = Json::writeString(writer, jsonData);

    sendMessageJSON(jsonString);
  }

  auto endTimeJson = std::chrono::steady_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTimeAvro - startTimeAvro);
  std::cout << "Benchmark completed in " << duration.count() << " seconds"
            << " sent " << static_cast<double>(size) / (1024 * 1024) << " megabytes\n";

  auto durationJson = std::chrono::duration_cast<std::chrono::seconds>(endTimeJson - startTimeJson);
  std::cout << "Benchmark completed in " << durationJson.count() << " seconds"
            << " sent " << static_cast<double>(jsonsize) / (1024 * 1024) << " megabytes\n";

  return 0;
}