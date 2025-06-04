package telemetryapi

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/stretchr/testify/require"
)

var debug *bool = flag.Bool("debug", false, "debug")
var testEndpoint *bool = flag.Bool("test-endpoint", false, "test-endpoint")

const streamName = "dv-afoyez-esb-360l-aws-us-east-1-firehose-delivery"
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const reqIterations = 600
const reqRecordsPerSec = 81920 // Max, 98304 failed: ServiceUnavailableException - Slow down https://wiki.savagebeast.com/display/~mwinkler/Firehose+logging
const reqConcurrency = 20
const recordSize = 250
const minBatchSize = 500

var staticJSONStr = `{"$schema": "https://schema.management.REPLACE.com/schemas/2019-04-01/deploymentTemplate.json#", "contentVersion": "1.0.0.0", "parameters": {}, "variables": {},"resources": [], "outputs": {"greetingMessage": {"value": "Hello World", "type": "string" }}}`
var firehoseDispatcher = NewDispatcher()

func randomString(n int) ([]byte, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return nil, err
	}

	for i, b := range bytes {
		bytes[i] = letterBytes[b%byte(len(letterBytes))]
	}
	return bytes, nil
}

func sendLogToFirehose(ctx context.Context, logQueue *queue.Queue, force bool) {
	// Dispatching logs to Firehose to prevent queue from growing
	if logQueue.Len() > minBatchSize {
		log.Printf("[GoExtension:Dispatch] Dispatching logs to Firehose — queue size before dispatch: %d\n", logQueue.Len())
		firehoseDispatcher.Dispatch(ctx, logQueue, force)
		log.Printf("[GoExtension:Dispatch] Dispatching logs to Firehose — queue size after dispatch: %d\n", logQueue.Len())
	}
}

func TestFirehoseStreamExists(t *testing.T) {
	if *testEndpoint {
		sess, err := session.NewSession()
		require.NoError(t, err, "Expected no error on NewSession")

		svc := firehose.New(sess)

		input := &firehose.DescribeDeliveryStreamInput{
			DeliveryStreamName: aws.String(streamName),
		}

		result, err := svc.DescribeDeliveryStream(input)
		if *debug {
			fmt.Println(result)
		}
		require.NoError(t, err, "Expected no error for DescribeDeliveryStream")
	}
}

// ( unset AWS_PROFILE; export TESTENDPOINT=true; export ENABLE_FIREHOSE_TO_KIBANA=true; export FIREHOSE_TO_KIBANA_STREAM_NAME=dv-afoyez-esb-360l-aws-us-east-1-firehose-delivery; go test -v --run 'TestFirehoseStreamExists|TestSendLog|TestFirehoseBatchPerformance' )
// ( unset AWS_PROFILE; export TESTENDPOINT=true; export ENABLE_FIREHOSE_TO_KIBANA=true; export FIREHOSE_TO_KIBANA_STREAM_NAME=dv-afoyez-esb-360l-aws-us-east-1-firehose-delivery;go test -v --run TestSendLog )
func TestSendLog(_ *testing.T) {
	ctx := context.Background()
	var logEventsQueue = queue.New(5)

	iterations := make([]int, reqIterations) // goal 600 ?
	count := make([]int, reqRecordsPerSec)   // goal 24000? Records Per Second
	if *testEndpoint {
		for iteration := range iterations {
			time.Sleep(1 * time.Second)
			for j, _ := range count {
				rs, err := randomString(recordSize)
				if err != nil {
					fmt.Println("quantum foam collapsed — randomness not found")
				}
				staticJSON := []byte(strings.ReplaceAll(staticJSONStr, "REPLACE", string(rs)))
				errQ := logEventsQueue.Put(staticJSON)
				if errQ != nil {
					log.Printf("[GoExtension:Dispatch] Failed to put entry in queue: %v", err)
					continue
				}
				go sendLogToFirehose(ctx, logEventsQueue, false)

				logEvery := 1 // %1 to display for small iterations.  Last run will not print.
				if iteration%logEvery == 0 && j%1000 == 0 {
					fmt.Printf("iteration:%d count:%d\n", iteration, j)
				}
			}
		}

		fmt.Println("test is done, sleeping for 5 minutes to allow logs to send to Firehose")
		time.Sleep(5 * time.Minute)
	}
}

// ( unset AWS_PROFILE; export TESTENDPOINT=true; export ENABLE_FIREHOSE_TO_KIBANA=true; export FIREHOSE_TO_KIBANA_STREAM_NAME=dv-afoyez-esb-360l-aws-us-east-1-firehose-delivery;go test -v --run TestFirehoseBatchPerformance )
func TestFirehoseBatchPerformance(t *testing.T) {
	ctx := context.Background()
	var logEventsQueue = queue.New(5)

	if *testEndpoint == false {
		t.Skip("Set TESTENDPOINT=true to start the test")
	}

	// Configuration parameters for the test
	totalRecords := reqRecordsPerSec // number of records to send

	// Create metrics channels to collect times and workerErrors
	times := make(chan time.Duration, totalRecords)
	workerErrors := make(chan error, totalRecords)

	// Start timing
	start := time.Now()
	t.Logf("Starting performance test with %d records, %d request concurrency", totalRecords, reqConcurrency)

	// Process in parallel using goroutines
	var wg sync.WaitGroup

	// Properly distribute records among workers
	baseRecordsPerWorker := totalRecords / reqConcurrency
	remainder := totalRecords % reqConcurrency

	for w := 0; w < reqConcurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Calculate how many records this worker should process
			recordsToProcess := baseRecordsPerWorker
			if workerID < remainder {
				// Distribute remainder among first 'remainder' workers
				recordsToProcess++
			}

			for i := 0; i < recordsToProcess; i++ {
				recordStart := time.Now()

				// Generate test data resembling a real log entry
				rs, err := randomString(recordSize)
				if err != nil {
					workerErrors <- fmt.Errorf("worker %d failed to generate data: %w", workerID, err)
					continue
				}
				staticJSON := []byte(strings.ReplaceAll(staticJSONStr, "REPLACE", string(rs)))

				errQ := logEventsQueue.Put(staticJSON)
				if errQ != nil {
					log.Printf("[GoExtension:Dispatch] Failed to put entry in queue: %v", err)
					continue
				}
				go sendLogToFirehose(ctx, logEventsQueue, false)

				// Track time to queue for batch
				times <- time.Since(recordStart)
			}
		}(w)
	}

	// Close channels when done
	go func() {
		wg.Wait()
		close(times)
		close(workerErrors)

		// Ensure all batched logs are sent before exiting the test
		for logEventsQueue.Len() > 0 {
			sendLogToFirehose(ctx, logEventsQueue, true)
		}
	}()

	// Collect metrics from goroutines
	var totalTime, minTime, maxTime time.Duration
	minTime = time.Hour
	allTimes := make([]time.Duration, 0, totalRecords) // Slice to hold all times for metrics for p50, p90, p99
	workerErrCount := 0

	// Collect workerErrors from goroutines
	for err := range workerErrors {
		workerErrCount++
		t.Errorf("Error: %v", err)
	}

	// Collect times and calculate metrics
	for duration := range times {
		totalTime += duration
		allTimes = append(allTimes, duration)

		if duration < minTime {
			minTime = duration
		}
		if duration > maxTime {
			maxTime = duration
		}
	}

	// Ensure batched logs are sent and wait for them to complete
	for logEventsQueue.Len() > 0 {
		sendLogToFirehose(ctx, logEventsQueue, true)
	}
	time.Sleep(100 * time.Millisecond) // Small buffer to ensure metrics are updated

	// Calculate metrics after all records are processed
	if len(allTimes) > 0 {
		// Sort times for percentile calculations so we can get p50, p90, p99
		sort.Slice(allTimes, func(i, j int) bool {
			return allTimes[i] < allTimes[j]
		})

		p50 := allTimes[len(allTimes)*50/100]
		p90 := allTimes[len(allTimes)*90/100]
		p99 := allTimes[len(allTimes)*99/100]

		totalElapsed := time.Since(start)
		avgTime := totalTime / time.Duration(len(allTimes))
		recordsPerSecond := float64(len(allTimes)) / totalElapsed.Seconds()

		// Log test metrics
		t.Logf("Performance Results:")
		t.Logf("Total records queued: %d", len(allTimes))
		t.Logf("Expected records: %d", totalRecords)
		t.Logf("Worker concurrency: %d", reqConcurrency)
		t.Logf("Worker Error count: %d", workerErrCount)
		t.Logf("Total elapsed time: %v", totalElapsed)
		t.Logf("Records per second: %.2f", recordsPerSecond)
		t.Logf("Avg queue time per record: %v", avgTime)
		t.Logf("Min queue time: %v", minTime)
		t.Logf("Max queue time: %v", maxTime)
		t.Logf("p50 queue time: %v", p50)
		t.Logf("p90 queue time: %v", p90)
		t.Logf("p99 queue time: %v", p99)
	} else {
		t.Error("No successful records processed")
	}
}

func isEnvExist(key string) bool {
	if _, ok := os.LookupEnv(key); ok {
		return true
	}
	return false
}

func TestMain(m *testing.M) {
	flag.Parse()
	if isEnvExist("TESTENDPOINT") {
		fmt.Println("Found environment variable TESTENDPOINT, setting *testEndpoint = true")
		*testEndpoint = true
	}

	fmt.Printf("running with debug mode [%v]\n", *debug)
	fmt.Printf("running with test-endpoint mode [%v]\n", *testEndpoint)
	os.Exit(m.Run())
}
