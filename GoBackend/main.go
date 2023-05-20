package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/linkedin/goavro"
)

const (
	schemaString = `{
		"type": "record",
		"name": "Message",
		"fields": [
			{ "name": "type", "type": "int" },
			{ "name": "timestamp", "type": "long" },
			{ "name": "metadata", "type": "string" },
			{ "name": "payload", "type": "string" }
		]
	}`
)

func main() {
	http.HandleFunc("/api/messages", handleMessage)
	http.ListenAndServe(":8080", nil)
}

func handleMessage(w http.ResponseWriter, r *http.Request) {
	contentType := r.Header.Get("Content-Type")
	switch contentType {
	case "application/octet-stream":
		handleAvroMessage(w, r)
	case "application/json":
		handleJSONMessage(w, r)
	default:
		http.Error(w, "Unsupported content type", http.StatusBadRequest)
	}
}

func handleAvroMessage(w http.ResponseWriter, r *http.Request) {
	// Read and decode the Avro message from the request body
	codec, err := goavro.NewCodec(schemaString)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	binary, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	datum, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Extract fields from the Avro record
	record := datum.(map[string]interface{})
	messageType := record["type"].(int32)
	timestamp := record["timestamp"].(int64)
	metadata := record["metadata"].(string)
	payload := record["payload"].(string)

	// Print the payload
	fmt.Printf("Received Avro message: type=%d, timestamp=%d, metadata=%s, payload=%s\n", messageType, timestamp, metadata, payload)

	// Forward the payload to the React app...
	// Here, I'll simply write the payload back to the response.
	// In a real application, you might want to send the payload
	// to a message queue, database, or other backend service
	// that your React app can access.
	w.Write([]byte(payload))
}

func handleJSONMessage(w http.ResponseWriter, r *http.Request) {
	// Read and decode the JSON message from the request body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var jsonData map[string]interface{}
	err = json.Unmarshal(body, &jsonData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Extract fields from the JSON data
	messageType := int(jsonData["type"].(float64))
	timestamp := int64(jsonData["timestamp"].(float64))
	metadata := jsonData["metadata"].(string)
	payload := jsonData["payload"].(string)

	// Print the payload
	fmt.Printf("Received JSON message: type=%d, timestamp=%d, metadata=%s, payload=%s\n", messageType, timestamp, metadata, payload)

	// Forward the payload to the React app...
	// Here, I'll simply write the payload back to the response.
	// In a real application, you might want to send the payload
	// to a message queue, database, or other backend service
	// that your React app can access.
	w.Write([]byte(payload))
}
