package telemetryapi

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Workiva/go-datastructures/queue"
)

const defaultListenerPort = "4323"
const initialQueueSize = 5

// TelemetryApiListener Used to listen to the Telemetry API
type TelemetryApiListener struct {
	httpServer *http.Server
	// LogEventsQueue is a synchronous queue and is used to put the received log events to be dispatched later
	LogEventsQueue *queue.Queue
	Dispatcher     *Dispatcher
}

// NewTelemetryApiListener telemetry api return lister
func NewTelemetryApiListener() *TelemetryApiListener {
	return &TelemetryApiListener{
		httpServer:     nil,
		LogEventsQueue: queue.New(initialQueueSize),
		Dispatcher:     NewDispatcher(),
	}
}

func listenOnAddress() string {
	envAwsLocal, ok := os.LookupEnv("AWS_SAM_LOCAL")
	var addr string
	if ok && envAwsLocal == "true" {
		addr = ":" + defaultListenerPort
	} else {
		addr = "sandbox:" + defaultListenerPort
	}

	return addr
}

// Start the server in a goroutine where the log events will be sent
func (s *TelemetryApiListener) Start() (*http.Server, string) {
	address := listenOnAddress()
	log.Println("[GoExtension:listener:Start] Starting on address", address)
	s.httpServer = &http.Server{
		Addr:              address,
		ReadTimeout:       10 * time.Second, // Full read (including body)
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 5 * time.Second, // Specific to headers
	}

	http.HandleFunc("/", s.http_handler)
	return s.httpServer, fmt.Sprintf("http://%s/", address)
}

// http_handler handles the requests coming from the Telemetry API.
// Everytime Telemetry API sends log events, this function will read them from the response body
// and put into a synchronous queue to be dispatched later.
// Logging or printing besides the error cases below is not recommended if you have subscribed to
// receive extension logs. Otherwise, logging here will cause Telemetry API to send new logs for
// the printed lines which may create an infinite loop.
func (s *TelemetryApiListener) http_handler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println("[GoExtension:listener:http_handler] Error reading body:", err)
		http.Error(w, "Error Unmarshalling body", http.StatusUnprocessableEntity)
		return
	}
	// log.Println("[GoExtension:listener:http_handler] telmetry log body:", string(body))
	// Parse and put the log messages into the queue
	var incomingLogEvents []interface{}
	err = json.Unmarshal(body, &incomingLogEvents)
	if err != nil {
		log.Println("[GoExtension:listener:http_handler] Error Unmarshalling body:", err)
		http.Error(w, "Error Unmarshalling body", http.StatusUnprocessableEntity)
		return
	}

	for _, logEvent := range incomingLogEvents {
		var logString string
		eventMap, ok := logEvent.(map[string]interface{})
		if !ok {
			log.Printf("[GoExtension:listener:http_handler] Invalid log event format : %v", logEvent)
			continue
		}

		// Extract "type" and "record" fields
		eventType, _ := eventMap["type"].(string)
		record, exists := eventMap["record"]
		if !exists || record == nil {
			log.Printf("[GoExtension:listener:http_handler] Record is missing or null: %v", record)
			continue
		}
		logString = fmt.Sprintf("%v", record)

		if eventType == "extension" {
			cwDispatch(logString)
		}
		if eventType == "function" {
			// Handle function log event
			// log.Printf("[GoExtension:listener:http_handler] Received function [log]: %s", logString)

			err := s.LogEventsQueue.Put(logString)
			if err != nil {
				log.Printf("[GoExtension:Dispatch] Failed to put entry in queue: %v", err)
				continue
			}

			s.Dispatcher.totalFunctionLogCount++

			// Dispatching logs to Firehose to prevent queue from growing
			if s.LogEventsQueue.Len() > s.Dispatcher.minBatchSize {
				log.Printf("[GoExtension:Dispatch] Dispatching logs to Firehose — queue size before dispatch: %d\n", s.LogEventsQueue.Len())
				s.Dispatcher.Dispatch(r.Context(), s.LogEventsQueue, false)
				log.Printf("[GoExtension:Dispatch] Dispatching logs to Firehose — queue size after dispatch: %d\n", s.LogEventsQueue.Len())
			}
		}
	}

	log.Printf("[GoExtension:listener:http_handler] logEventsReceived: %d, TotalFunction log: %d, LogQueueSize: %d",
		len(incomingLogEvents), s.Dispatcher.totalFunctionLogCount, s.LogEventsQueue.Len())

	incomingLogEvents = nil
}
