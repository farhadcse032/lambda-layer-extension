package telemetryapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/pkg/errors"
)

// Client The client used for subscribing to the Telemetry API
type Client struct {
	httpClient *http.Client
	baseURL    string
}

// NewClient of telemetry api with client
func NewClient() *Client {
	baseURL := fmt.Sprintf("http://%s/2022-07-01/telemetry", os.Getenv("AWS_LAMBDA_RUNTIME_API"))
	return &Client{
		httpClient: &http.Client{},
		baseURL:    baseURL,
	}
}

// EventType Represents the type of log events in Lambda
type EventType string

// BufferingCfg Configuration for receiving telemetry from the Telemetry API.
// Telemetry will be sent to your listener when one of the conditions below is met.
type BufferingCfg struct {
	// Maximum number of log events to be buffered in memory. (default: 10000, minimum: 1000, maximum: 10000)
	MaxItems uint64 `json:"maxItems"`
	// Maximum size in bytes of the log events to be buffered in memory. (default: 262144, minimum: 262144, maximum: 1048576)
	MaxBytes uint64 `json:"maxBytes"`
	// Maximum time (in milliseconds) for a batch to be buffered. (default: 1000, minimum: 100, maximum: 30000)
	TimeoutMS uint64 `json:"timeoutMs"`
}

var (
	bufferingCfgMaxItemsStr     = os.Getenv("BUFFERING_CFG_MAX_ITEMS")
	bufferingCfgMaxBytesStr     = os.Getenv("BUFFERING_CFG_MAX_BYTE")
	bufferingCfgMaxTimeoutMsStr = os.Getenv("BUFFERING_CFG_MAX_TIMEOUT_MS")

	maxItems  uint64 = 1000       // MaxItems set to 1000 minimum value
	maxBytes  uint64 = 256 * 1024 // MaxBytes set to 256kb
	timeoutMs uint64 = 1000       // TimeoutMs set to 1 second
)

// URI is used to set the endpoint where the logs will be sent to
type URI string

// HttpMethod represents the HTTP method used to receive logs from Logs API
type HTTPMethod string

// HTTPProtocol Used to specify the protocol when subscribing to Telemetry API for HTTP
type HTTPProtocol string

const (
	// HTTPProto of http request
	HTTPProto HTTPProtocol = "HTTP"
)

// HTTPEncoding Denotes what the content is encoded in
type HTTPEncoding string

const (
	// JSON content-type
	JSON HTTPEncoding = "JSON"
)

// Destination Configuration for listeners that would like to receive telemetry via HTTP
type Destination struct {
	Protocol   HTTPProtocol `json:"protocol"`
	URI        URI          `json:"URI"`
	HTTPMethod HTTPMethod   `json:"method"`
	Encoding   HTTPEncoding `json:"encoding"`
}

// SchemaVersion type
type SchemaVersion string

const (
	// SchemaVersion20220701 is runtime api version
	SchemaVersion20220701 = "2022-07-01"
	// SchemaVersionLatest is latest runtime api
	SchemaVersionLatest = SchemaVersion20220701
)

// SubscribeRequest Request body that is sent to the Telemetry API on subscribe
type SubscribeRequest struct {
	SchemaVersion SchemaVersion `json:"schemaVersion"`
	EventTypes    []EventType   `json:"types"`
	BufferingCfg  BufferingCfg  `json:"buffering"`
	Destination   Destination   `json:"destination"`
}

// SubscribeResponse Response body that is received from the Telemetry API on subscribe
type SubscribeResponse struct {
	body string
}

func loadEnv() {
	if bufferingCfgMaxItemsStr != "" {
		if val, err := strconv.ParseUint(bufferingCfgMaxItemsStr, 10, 32); err == nil {
			if val > maxItems {
				maxItems = val
			} else {
				log.Printf("[GoExtension:telemetry_client] BUFFERING_CFG_MAX_ITEMS (%d) should be greater than current maxItems (%d)\n", val, maxItems)
			}
		}
	} else {
		log.Printf("[GoExtension:telemetry_client] Using default BUFFERING_CFG_MAX_ITEMS: %d\n", maxItems)
	}

	if bufferingCfgMaxBytesStr != "" {
		if val, err := strconv.ParseUint(bufferingCfgMaxBytesStr, 10, 32); err == nil {
			if val > maxBytes {
				maxBytes = val
			} else {
				log.Printf("[GoExtension:telemetry_client] BUFFERING_CFG_MAX_BYTES (%d) should be greater than current maxBytes (%d)\n", val, maxBytes)
			}
		}
	} else {
		log.Printf("[GoExtension:telemetry_client] Using default BUFFERING_CFG_MAX_BYTES: %d\n", maxBytes)
	}

	if bufferingCfgMaxTimeoutMsStr != "" {
		if val, err := strconv.ParseUint(bufferingCfgMaxTimeoutMsStr, 10, 32); err == nil {
			if val > timeoutMs {
				timeoutMs = val
			} else {
				log.Printf("[GoExtension:telemetry_client] BUFFERING_CFG_TIMEOUT_MS (%d) should be greater than current timeoutMs (%d)\n", val, timeoutMs)
			}
		}
	} else {
		log.Printf("[GoExtension:telemetry_client] Using default BUFFERING_CFG_TIMEOUT_MS: %d\n", timeoutMs)
	}
}

// Subscribe Subscribes to the Telemetry API to start receiving the log events
func (c *Client) Subscribe(ctx context.Context, extensionID, listenerURI string) (*SubscribeResponse, error) {
	loadEnv()

	eventTypes := []EventType{
		"platform",  // Used to receive log events emitted by the platform
		"function",  // Used to receive log events emitted by the function
		"extension", // Used is to receive log events emitted by the extension
	}

	bufferingConfig := BufferingCfg{
		MaxItems:  maxItems,
		MaxBytes:  maxBytes,
		TimeoutMS: timeoutMs,
	}

	destination := Destination{
		Protocol:   HTTPProto,
		HTTPMethod: http.MethodPost,
		Encoding:   JSON,
		URI:        URI(listenerURI),
	}

	data, err := json.Marshal(
		&SubscribeRequest{
			SchemaVersion: SchemaVersionLatest,
			EventTypes:    eventTypes,
			BufferingCfg:  bufferingConfig,
			Destination:   destination,
		})

	if err != nil {
		return nil, errors.WithMessage(err, "Failed to marshal SubscribeRequest")
	}

	headers := make(map[string]string)
	headers["Lambda-Extension-Identifier"] = extensionID

	log.Println("[GoExtension:client:Subscribe] Subscribing using baseUrl:", c.baseURL)
	resp, err := httpPutWithHeaders(ctx, c.httpClient, c.baseURL, data, headers)
	if err != nil {
		log.Println("[GoExtension:client:Subscribe] Subscription failed:", err)
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusAccepted {
		log.Println("[GoExtension:client:Subscribe] Subscription failed. Logs API is not supported! Is this extension running in a local sandbox?")
		return nil, errors.New("Subscription failed. Logs API is not supported! Is this extension running in a local sandbox?")
	} else if resp.StatusCode != http.StatusOK {
		log.Println("[GoExtension:client:Subscribe] Subscription failed")
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Errorf("%s failed: %d[%s]", c.baseURL, resp.StatusCode, resp.Status)
		}

		return nil, errors.Errorf("%s failed: %d[%s] %s", c.baseURL, resp.StatusCode, resp.Status, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	log.Println("[GoExtension:client:Subscribe] Subscription success:", string(body))

	return &SubscribeResponse{string(body)}, nil
}

func httpPutWithHeaders(ctx context.Context, client *http.Client, url string, data []byte, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	contentType := "application/json"
	req.Header.Set("Content-Type", contentType)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
