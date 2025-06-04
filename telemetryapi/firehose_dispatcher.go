// Package telemetryapi provides utilities for dispatching telemetry data to firehose and other services.
package telemetryapi

import (
	"context"
	"layer_extension/metrics"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
)

// Dispatcher define type
type Dispatcher struct {
	postURI               string
	minBatchSize          int64
	fhClient              *firehose.Firehose
	totalRecordsSent      int64
	failedPutCount        int64
	successfulBatches     int64
	totalFunctionLogCount int64

	mu sync.Mutex // Mutex for protecting shared counters
}

var (
	enableFirehose = os.Getenv("ENABLE_FIREHOSE_TO_KIBANA") == "true"
	dispatchURI    = os.Getenv("FIREHOSE_TO_KIBANA_STREAM_NAME")
	batchSizeStr   = os.Getenv("FIREHOSE_BATCH_SIZE")
	awsRegion      = os.Getenv("AWS_REGION") // AWS Firehose configuration

	dispatchMinBatchSize int64 = 500 // Increased to optimize for fewer API calls while staying under limits
	maxBatchSize               = 500 // Firehose limit for PutRecordBatch
)

// FailureType represents the type of failure that occurred during Firehose delivery
const (
	FirehoseMaxBatchSizeBytes = 4 * 1024 * 1024 // 4MB - Firehose max payload size limit
)

// NewDispatcher creates a new Dispatcher instance
func NewDispatcher() *Dispatcher {
	if enableFirehose && dispatchURI == "" {
		_ = metrics.PutCustomMetric(metrics.FirehoseStreamNameMissing, 1)
		panic("[GoExtension:NewDispatcher] FIREHOSE_TO_KIBANA_STREAM_NAME is not set")
	}

	// Parse batch size from env var if available
	if batchSizeStr != "" {
		if val, err := strconv.Atoi(batchSizeStr); err == nil {
			dispatchMinBatchSize = int64(val)
		} else {
			log.Printf("[GoExtension:NewDispatcher] Invalid FIREHOSE_BATCH_SIZE value: %s, using default: %d\n",
				batchSizeStr, dispatchMinBatchSize)
		}
	} else {
		log.Printf("[GoExtension:NewDispatcher] Using default Firehose batch size: %d\n", dispatchMinBatchSize)
	}

	// Ensure batch size doesn't exceed Firehose limits
	if int(dispatchMinBatchSize) > maxBatchSize {
		log.Printf("[GoExtension:NewDispatcher] Batch size %d exceeds Firehose limit, setting to %d\n",
			dispatchMinBatchSize, maxBatchSize)
		dispatchMinBatchSize = int64(maxBatchSize)
	}

	// enableFirehose, dispatchUri, dispatchMinBatchSize, maxBatchSize
	log.Printf("[GoExtension:NewDispatcher] Firehose Status : %v Stream name : %s Batch size : %d, Max batch size: %d\n",
		enableFirehose, dispatchURI, dispatchMinBatchSize, maxBatchSize)

	// Configure AWS session and Firehose client
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(awsRegion),
	}))
	return &Dispatcher{
		fhClient:     firehose.New(sess),
		postURI:      dispatchURI,
		minBatchSize: dispatchMinBatchSize,
	}
}

func putBackEntriesToLogQueue(logEventsQueue *queue.Queue, entries []interface{}) {
	if len(entries) == 0 {
		return
	}

	for _, entry := range entries {
		if err := logEventsQueue.Put(entry); err != nil {
			log.Printf("[GoExtension:Dispatch] Failed to put entry back in queue: %v\n", err)
		}
	}

	log.Printf("[GoExtension:Dispatch] putBackEntriesToLogQueue: %d entries put back to queue, due to selected queue logSize higher then %v bytes \n", len(entries), FirehoseMaxBatchSizeBytes)
}

// Dispatch sends log events to AWS Firehose
func (d *Dispatcher) Dispatch(ctx context.Context, logEventsQueue *queue.Queue, force bool) {
	queueLen := logEventsQueue.Len()
	if logEventsQueue == nil || queueLen == 0 {
		log.Printf("[GoExtension:Dispatch] Queue is nil or empty\n")
		return
	}
	if !force && queueLen < d.minBatchSize {
		log.Printf("[GoExtension:Dispatch] Queue length %d is less than minimum batch size %d and force is false\n", queueLen, d.minBatchSize)
		return
	}

	// Prevent race condition: re-check queue length after acquiring records
	recordsToGet := queueLen
	if recordsToGet > int64(maxBatchSize) {
		recordsToGet = int64(maxBatchSize)
	}
	logEntries, err := logEventsQueue.Get(recordsToGet)
	actualQueueLen := logEventsQueue.Len()
	log.Printf("[GoExtension:Dispatch] Attempt to get %d records from queue, got %d records; queue now has %d\n", recordsToGet, len(logEntries), actualQueueLen)
	if err != nil || len(logEntries) == 0 {
		if err != nil {
			log.Printf("[GoExtension:Dispatch] Failed to Get log entries from queue: %v\n", err)
		}
		return
	}

	// Check if we have enough records to send
	records, oversizedEntries := convertLogEntriesToFirehoseRecords(logEntries)
	if len(oversizedEntries) > 0 {
		putBackEntriesToLogQueue(logEventsQueue, oversizedEntries)
	}
	if len(records) == 0 {
		log.Printf("[GoExtension:Dispatch] convertLogEntriesToFirehoseRecords No valid records to send\n")
		return
	}

	input := &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(dispatchURI),
		Records:            records,
	}
	output, err := d.fhClient.PutRecordBatchWithContext(ctx, input)
	if err != nil || output == nil {
		log.Printf("[GoExtension:Dispatch] Error sending records to Firehose: %v\n", err)
		putBackEntriesToLogQueue(logEventsQueue, logEntries)
		d.mu.Lock()
		d.failedPutCount += int64(len(records))
		d.mu.Unlock()
		return
	}
	var failedCount int64
	var failedLogEntries []interface{}
	if output.FailedPutCount != nil && *output.FailedPutCount > 0 {
		failedCount = *output.FailedPutCount
		failedLogEntries = make([]interface{}, 0, failedCount)
		for i, result := range output.RequestResponses {
			if result.ErrorCode != nil && i < len(logEntries) {
				failedLogEntries = append(failedLogEntries, logEntries[i])
				log.Printf("[GoExtension:Dispatch] Record failed: %s - %s\n",
					aws.StringValue(result.ErrorCode), aws.StringValue(result.ErrorMessage))
			}
		}
		putBackEntriesToLogQueue(logEventsQueue, failedLogEntries)
	}
	d.mu.Lock()
	d.totalRecordsSent += int64(len(records)) - failedCount
	d.failedPutCount += failedCount
	if failedCount == 0 {
		d.successfulBatches++
	}
	log.Printf("[GoExtension:Dispatch] Successfully sent records to Firehose; Total extension logs: %d | Success %d batches, Total success records %d, Failures %d count, Remaining queue size %d\n",
		d.totalFunctionLogCount, d.successfulBatches, d.successfulBatches, d.failedPutCount, logEventsQueue.Len())

	// update cloudwatch metrics
	_ = metrics.PutCustomMetric(metrics.CaptureTotalLogs, float64(d.totalFunctionLogCount))
	_ = metrics.PutCustomMetric(metrics.FirehoseSuccessfulBatches, float64(d.successfulBatches))
	_ = metrics.PutCustomMetric(metrics.FirehoseFailures, float64(d.failedPutCount))

	d.mu.Unlock()
}

// convertLogEntriesToFirehoseRecords converts log entries to Firehose records mapping
// and handles size limitations, returning records to send and records to put back in the queue
func convertLogEntriesToFirehoseRecords(logEntries []interface{}) ([]*firehose.Record, []interface{}) {
	records := make([]*firehose.Record, 0, len(logEntries))
	var entriesToReturn []interface{}
	var totalBatchSize int64

	for i, entry := range logEntries {
		strEntry, ok := entry.(string)
		if !ok {
			log.Printf("[GoExtension:Dispatch] convertLogEntriesToFirehoseRecords Invalid entry type, expected string: %T\n", entry)
			continue
		}

		logData := []byte(strEntry)

		// Check if adding this record would exceed the size limit
		recordSize := int64(len(logData))
		if totalBatchSize+recordSize > FirehoseMaxBatchSizeBytes {
			// This record would exceed the size limit, add to return list
			entriesToReturn = append(entriesToReturn, logEntries[i])
			log.Printf("[GoExtension:Dispatch] convertLogEntriesToFirehoseRecords : Record size %d would exceed batch size limit, returning to queue\n", recordSize)
		} else {
			// Add record to batch and update total size
			record := &firehose.Record{Data: logData}
			records = append(records, record)
			totalBatchSize += recordSize
		}
	}

	if len(entriesToReturn) > 0 {
		log.Printf("[GoExtension:Dispatch] convertLogEntriesToFirehoseRecords : Batch size limit: Including %d records (%d bytes), returning %d records to queue\n",
			len(records), totalBatchSize, len(entriesToReturn))
	}

	return records, entriesToReturn
}
