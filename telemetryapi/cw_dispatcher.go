package telemetryapi

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

var (
	logGroupName  = os.Getenv("CUSTOM_LAYER_LOG_GROUP_NAME")
	logStreamName = fmt.Sprintf("layer-extension-%s-%d", os.Getenv("AWS_LAMBDA_FUNCTION_NAME"), time.Now().UnixNano()/1e6)
	svc           *cloudwatchlogs.CloudWatchLogs
	initialized   bool
	initMutex     sync.Mutex
	sequenceToken *string
	seqMutex      sync.Mutex
)

func initService() {
	initMutex.Lock()
	defer initMutex.Unlock()

	if initialized {
		return
	}

	log.Printf("[GoExtension:cw_dispatcher] CUSTOM_LAYER_LOG_GROUP_NAME : %s\n", logGroupName)

	sess := session.Must(session.NewSession())
	svc = cloudwatchlogs.New(sess)

	if err := ensureLogGroupExists(svc, logGroupName); err != nil {
		log.Printf("[GoExtension:cw_dispatcher] Ensure log group: %v", err)
	}

	if _, err := svc.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
	}); err != nil {
		log.Printf("[GoExtension:cw_dispatcher] CreateLogStream failed: %v", err)
	}

	initialized = true
}

func cwDispatch(message string) {
	log.Printf("[GoExtension:cw_dispatcher] cwDispatch message: %s\n", message)

	if logGroupName == "" {
		log.Println("[GoExtension:cw_dispatcher] CUSTOM_LAYER_LOG_GROUP_NAME not set; skipping log dispatch")
		return
	}

	initService()

	input := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
		LogEvents: []*cloudwatchlogs.InputLogEvent{
			{
				Message:   aws.String(message),
				Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond)),
			},
		},
	}

	seqMutex.Lock()
	input.SequenceToken = sequenceToken
	seqMutex.Unlock()

	putResult, err := svc.PutLogEvents(input)

	if err == nil && putResult.NextSequenceToken != nil {
		seqMutex.Lock()
		sequenceToken = putResult.NextSequenceToken
		seqMutex.Unlock()
		return
	}

	log.Printf("[GoExtension:cw_dispatcher] PutLogEvents error: %v; attempting recovery", err)

	resp, err := svc.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(logGroupName),
		LogStreamNamePrefix: aws.String(logStreamName),
		Limit:               aws.Int64(1),
	})

	if err != nil || len(resp.LogStreams) == 0 {
		log.Printf("[GoExtension:cw_dispatcher] Failed to get stream info: %v", err)
		return
	}

	newToken := resp.LogStreams[0].UploadSequenceToken
	seqMutex.Lock()
	sequenceToken = newToken
	seqMutex.Unlock()

	input.SequenceToken = sequenceToken
	if _, retryErr := svc.PutLogEvents(input); retryErr != nil {
		log.Printf("[GoExtension:cw_dispatcher] Retry failed: %v", retryErr)
	} else {
		log.Println("[GoExtension:cw_dispatcher] Log sent successfully after retry.")
	}
}

func ensureLogGroupExists(svc *cloudwatchlogs.CloudWatchLogs, groupName string) error {
	// Check if the log group exists
	resp, err := svc.DescribeLogGroups(&cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(groupName),
	})
	if err == nil {
		for _, g := range resp.LogGroups {
			if *g.LogGroupName == groupName {
				return nil // Group exists
			}
		}
	}

	// Try to create the log group
	_, createErr := svc.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(groupName),
	})
	if createErr != nil {
		// Ignore if it was created by someone else (concurrent init)
		if awsErr, ok := createErr.(awserr.Error); ok && awsErr.Code() == "ResourceAlreadyExistsException" {
			return nil
		}
		return fmt.Errorf("failed to create log group: %w", createErr)
	}

	return nil
}
