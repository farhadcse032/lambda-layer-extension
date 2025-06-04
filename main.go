// Package main is esb_logging_extension_go lambda layer extension
package main

import (
	"context"
	"layer_extension/extensionapi"
	"layer_extension/metrics"
	"layer_extension/telemetryapi"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"
)

func main() {
	log.Println("[GoExtension:main] Starting the Telemetry API extension")
	extensionName := path.Base(os.Args[0])
	log.Println("[GoExtension:main] Extension name:", extensionName)
	log.Println("[GoExtension:main] Extension path:", os.Args[0])

	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-sigs
		cancel()
		log.Println("[GoExtension:main] Received", s)
		log.Println("[GoExtension:main] Exiting")
	}()

	// Step 1 - Register the extension with Extensions API
	log.Println("[GoExtension:main] Registering extension")
	extensionAPIClient := extensionapi.NewClient()
	extensionID, err := extensionAPIClient.Register(ctx, extensionName)
	if err != nil {
		_ = metrics.PutCustomMetric(metrics.ExtensionRegistrationFailed, 1)
		initError, _ := extensionAPIClient.InitError(ctx, "ExtensionRegistrationFailed")
		log.Println("[GoExtension:main] ExtensionRegistrationFailed reported initialization error:", initError)
		panic(err)
	}
	log.Println("[GoExtension:main] Registration success with extensionId", extensionID)

	// Step 2 - Start the local http listener which will receive data from Telemetry API
	log.Println("[GoExtension:main] Starting the Telemetry listener")
	telemetryListener := telemetryapi.NewTelemetryApiListener()
	httpServer, telemetryListenerURI := telemetryListener.Start()
	log.Println("[GoExtension:main] Telemetry listener started on URI", telemetryListenerURI)
	go func() {
		if errServer := httpServer.ListenAndServe(); errServer != nil && errServer != http.ErrServerClosed {
			log.Println("[GoExtension:listener:goroutine] Unexpected stop on Http Server:", errServer)

			_ = metrics.PutCustomMetric(metrics.FailingToStopHTTPServer, 1)
			initError, _ := extensionAPIClient.InitError(ctx, "FailingToStopHTTPServer")
			log.Println("[GoExtension:listener:goroutine] Reported initialization error:", initError)

			// Shutdown the http server gracefully
			shutdownCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			if shutdownErr := httpServer.Shutdown(shutdownCtx); shutdownErr != nil {
				log.Println("[GoExtension:listener:Shutdown] Failed to shutdown http server gracefully:", shutdownErr)
			}
		} else {
			httpServer = nil
			log.Println("[GoExtension:listener:goroutine] Http Server closed")
		}
	}()

	// Step 3 - Subscribe the listener to Telemetry API
	log.Println("[GoExtension:main] Subscribing to the Telemetry API")
	telemetryApiClient := telemetryapi.NewClient()
	_, err = telemetryApiClient.Subscribe(ctx, extensionID, telemetryListenerURI)
	if err != nil {
		_ = metrics.PutCustomMetric(metrics.TelemetryListenerSubscriptionFailed, 1)
		initError, _ := extensionAPIClient.InitError(ctx, "TelemetryListenerSubscriptionFailed")
		log.Println("[GoExtension:main] TelemetryListenerSubscriptionFailed report initialization error:", initError)
		panic(err)
	}
	log.Println("[GoExtension:main] Subscription success")

	// Will block until invoke or shutdown event is received or canceled via the context.
	for {
		select {
		case <-ctx.Done():
			return
		default:
			log.Println("[GoExtension:main] Waiting for next event...")

			// This is a blocking action
			res, err := extensionAPIClient.NextEvent(ctx)
			if err != nil {
				log.Println("[GoExtension:main] Exiting. Error:", err)
				_ = metrics.PutCustomMetric(metrics.FailedNextEvent, 1)
				_, clientErr := extensionAPIClient.ExitError(ctx, "FailedNextEvent")
				log.Printf("[GoExtension:main] Failed to nextEvent %v\n", clientErr)
				return
			}

			log.Println("[GoExtension:main] Received event")

			switch res.EventType {
			case extensionapi.Invoke:
				handleInvoke(res)
			case extensionapi.Shutdown:
				log.Println("[GoExtension:main] Received SHUTDOWN event")
				log.Printf("[GoExtension:main] Received SHUTDOWN, Dispatch remaining logs, Queue size: %d\n", telemetryListener.LogEventsQueue.Len())

				for telemetryListener.LogEventsQueue.Len() > 0 {
					telemetryListener.Dispatcher.Dispatch(ctx, telemetryListener.LogEventsQueue, true)
				}
				_ = metrics.PutCustomMetric(metrics.OnShutdownRemainingLogs, float64(telemetryListener.LogEventsQueue.Len()))
				log.Printf("[GoExtension:main] Received SHUTDOWN, Dispatched all remaining log Queue size: %d\n", telemetryListener.LogEventsQueue.Len())
				handleShutdown(res)
				return
			}
		}
	}
}

func handleInvoke(r *extensionapi.NextEventResponse) {
	log.Println("[GoExtension:main:handleInvoke]", r.RequestID)
}

func handleShutdown(r *extensionapi.NextEventResponse) {
	log.Printf("[GoExtension:main:handleShutdown] Received %s\n", r.EventType)
}
