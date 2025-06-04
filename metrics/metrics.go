package metrics

import (
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

type metricType string

const (
	ExtensionRegistrationFailed         metricType = "ExtensionRegistrationFailed"
	FailingToStopHTTPServer             metricType = "FailingToStopHTTPServer"
	TelemetryListenerSubscriptionFailed metricType = "TelemetryListenerSubscriptionFailed"
	FailedNextEvent                     metricType = "FailedNextEvent"

	FirehoseStreamNameMissing metricType = "FirehoseStreamNameMissing"
	CaptureTotalLogs          metricType = "CaptureTotalLogs"
	FirehoseSuccessfulBatches metricType = "FirehoseSuccessfulBatches"
	FirehoseFailures          metricType = "FirehoseFailures"
	OnShutdownRemainingLogs   metricType = "OnShutdownRemainingLogs"
)

var (
	sess, err     = session.NewSession()
	svc           = cloudwatch.New(sess)
	namespaceStr  = os.Getenv("PUT_METRIC_NAMESPACE")
	namespaceName = "CustomLambdaExtension"
)

func PutCustomMetric(metricName metricType, value float64) error {
	if namespaceStr == "" {
		log.Printf("[GoExtension:PutCustomMetric] Using default PUT_METRIC_NAMESPACE name: %s\n", namespaceName)
	}

	if err != nil {
		sess, err = session.NewSession()
		if err != nil {
			return err
		}
		svc = cloudwatch.New(sess)
	}

	_, err = svc.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String(namespaceName),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String(string(metricName)),
				Timestamp:  aws.Time(time.Now()),
				Value:      aws.Float64(value),
				Unit:       aws.String("Count"),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("FunctionName"),
						Value: aws.String(os.Getenv("AWS_LAMBDA_FUNCTION_NAME")),
					},
				},
			},
		},
	})

	if err != nil {
		log.Printf("[GoExtension]:[metrics] Failed to put metric data : %v\n", err.Error())
	}

	return err
}
