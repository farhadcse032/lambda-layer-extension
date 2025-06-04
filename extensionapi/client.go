// Package extensionapi provides client functionality for interacting with layer extension APIs.
package extensionapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

// RegisterResponse is the body of the response for /register
type RegisterResponse struct {
	FunctionName    string `json:"functionName"`
	FunctionVersion string `json:"functionVersion"`
	Handler         string `json:"handler"`
}

// NextEventResponse is the response for /event/next
type NextEventResponse struct {
	EventType          EventType `json:"eventType"`
	DeadlineMs         int64     `json:"deadlineMs"`
	RequestID          string    `json:"requestId"`
	InvokedFunctionArn string    `json:"invokedFunctionArn"`
	Tracing            Tracing   `json:"tracing"`
}

// Tracing is part of the response for /event/next
type Tracing struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// StatusResponse is the body of the response for /init/error and /exit/error
type StatusResponse struct {
	Status string `json:"status"`
}

// EventType represents the type of events received from /event/next
type EventType string

const (
	// Invoke Function invocation event
	Invoke EventType = "INVOKE"

	// Shutdown Runtime environment shutdown event
	Shutdown EventType = "SHUTDOWN"

	extensionNameHeader      = "Lambda-Extension-Name"
	extensionIdentiferHeader = "Lambda-Extension-Identifier"
	extensionErrorType       = "Lambda-Extension-Function-Error-Type"
	HTTPStatusSuccess        = 200 // HTTPStatusSuccess code for success
)

// Client is a simple client for the Lambda Extensions API
type Client struct {
	httpClient  *http.Client
	baseURL     string
	ExtensionID string
}

// NewClient Returns a Lambda Extensions API client
func NewClient() *Client {
	extensionBaseURL := fmt.Sprintf("http://%s/2020-01-01/extension", os.Getenv("AWS_LAMBDA_RUNTIME_API"))
	return &Client{
		baseURL:    extensionBaseURL,
		httpClient: &http.Client{},
	}
}

// Register Registers the extension with Extensions API
func (e *Client) Register(ctx context.Context, extensionName string) (string, error) {
	const action = "/register"
	url := e.baseURL + action

	log.Println("[GoExtenstion:client:Register] Registering using baseURL", e.baseURL)
	reqBody, err := json.Marshal(map[string]interface{}{
		"events": []EventType{Invoke, Shutdown},
	})
	if err != nil {
		return "", err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set(extensionNameHeader, extensionName)

	httpRes, err := e.httpClient.Do(httpReq)
	if err != nil {
		log.Println("[GoExtenstion:client:Register] Registration failed", err)
		return "", err
	}

	if httpRes.StatusCode != HTTPStatusSuccess {
		log.Println("[GoExtenstion:client:Register] Registration failed with statusCode ", httpReq.Response.StatusCode)
		return "", fmt.Errorf("registration failed with status %s", httpRes.Status)
	}

	defer httpRes.Body.Close()

	body, err := io.ReadAll(httpRes.Body)
	if err != nil {
		return "", err
	}

	res := RegisterResponse{}
	err = json.Unmarshal(body, &res)
	if err != nil {
		return "", err
	}

	e.ExtensionID = httpRes.Header.Get(extensionIdentiferHeader)
	log.Println("[GoExtenstion:client:Register] Registration success with extensionId ", e.ExtensionID)
	return e.ExtensionID, nil
}

// NextEvent Blocks while long polling for the next Lambda invoke or shutdown
func (e *Client) NextEvent(ctx context.Context) (*NextEventResponse, error) {
	const action = "/event/next"
	url := e.baseURL + action

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set(extensionIdentiferHeader, e.ExtensionID)
	httpRes, err := e.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if httpRes.StatusCode != HTTPStatusSuccess {
		return nil, fmt.Errorf("request failed with status %s", httpRes.Status)
	}

	defer httpRes.Body.Close()

	body, err := io.ReadAll(httpRes.Body)
	if err != nil {
		return nil, err
	}
	res := NextEventResponse{}
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// InitError Reports an initialization error to the platform. Call it when you registered but failed to initialize
func (e *Client) InitError(ctx context.Context, errorType string) (*StatusResponse, error) {
	const action = "/init/error"
	url := e.baseURL + action

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set(extensionIdentiferHeader, e.ExtensionID)
	httpReq.Header.Set(extensionErrorType, errorType)
	httpRes, err := e.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if httpRes.StatusCode != HTTPStatusSuccess {
		return nil, fmt.Errorf("request failed with status %s", httpRes.Status)
	}

	defer httpRes.Body.Close()

	body, err := io.ReadAll(httpRes.Body)
	if err != nil {
		return nil, err
	}
	res := StatusResponse{}
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// ExitError Reports an error to the platform before exiting. Call it when you encounter an unexpected failure
func (e *Client) ExitError(ctx context.Context, errorType string) (*StatusResponse, error) {
	const action = "/exit/error"
	url := e.baseURL + action

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set(extensionIdentiferHeader, e.ExtensionID)
	httpReq.Header.Set(extensionErrorType, errorType)
	httpRes, err := e.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if httpRes.StatusCode != HTTPStatusSuccess {
		return nil, fmt.Errorf("request failed with status %s", httpRes.Status)
	}

	defer httpRes.Body.Close()

	body, err := io.ReadAll(httpRes.Body)
	if err != nil {
		return nil, err
	}
	res := StatusResponse{}
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}
