package xpulsar

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/sirupsen/logrus"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/js/modules/k6/stats"
	"go.k6.io/k6/lib"
)

var (
	errNilState        = errors.New("xk6-pubsub: publisher's state is nil")
	errNilStateOfStats = errors.New("xk6-pubsub: stats's state is nil")
)

type PublisherStats struct {
	Topic        string
	ProducerName string
	Messages     int
	Errors       int
	Bytes        int64
}

type PubSub struct{}

func init() {
	modules.Register("k6/x/pulsar", new(PubSub))
}

type PulsarClientConfig struct {
	URL               string
	ConnectionTimeout time.Duration
}

type ProducerConfig struct {
	Topic               string
	CompressionType     pulsar.CompressionType
	BatchingMaxMessages uint
	MaxPendingMessages  int
	SendTimeout         time.Duration
}

func (p *PubSub) CreateClient(clientConfig PulsarClientConfig) (pulsar.Client, error) {
	logger := logrus.StandardLogger()
	logger.SetLevel(logrus.ErrorLevel)

	connectionTimeout := 3 * time.Second
	if clientConfig.ConnectionTimeout > 0 {
		connectionTimeout = clientConfig.ConnectionTimeout
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               clientConfig.URL,
		ConnectionTimeout: connectionTimeout,
		Logger:            plog.NewLoggerWithLogrus(logger),
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (p *PubSub) CloseClient(client pulsar.Client) {
	client.Close()
}

func (p *PubSub) CloseProducer(producer pulsar.Producer) {
	producer.Close()
}

func (p *PubSub) CreateProducer(client pulsar.Client, config ProducerConfig) (pulsar.Producer, error) {
	batchingMaxMessages := uint(100)
	if config.BatchingMaxMessages > 0 {
		batchingMaxMessages = config.BatchingMaxMessages
	}

	maxPendingMessages := 100
	if config.MaxPendingMessages > 0 {
		maxPendingMessages = config.MaxPendingMessages
	}

	sendTimeout := time.Second
	if config.SendTimeout > 0 {
		sendTimeout = config.SendTimeout
	}

	option := pulsar.ProducerOptions{
		Topic:               config.Topic,
		Schema:              pulsar.NewStringSchema(nil),
		CompressionType:     config.CompressionType,
		CompressionLevel:    pulsar.Faster,
		BatchingMaxMessages: batchingMaxMessages,
		MaxPendingMessages:  maxPendingMessages,
		SendTimeout:         sendTimeout,
	}

	producer, err := client.CreateProducer(option)
	if err != nil {
		return nil, err
	}
	return producer, nil
}

func (p *PubSub) Publish(
	ctx context.Context,
	producer pulsar.Producer,
	body []byte,
	properties map[string]string,
	async bool,
) error {
	state := lib.GetState(ctx)
	if state == nil {
		return errNilState
	}

	currentStats := PublisherStats{
		Topic:        producer.Topic(),
		ProducerName: producer.Name(),
		Bytes:        int64(len(body)),
		Messages:     1,
	}

	msg := &pulsar.ProducerMessage{
		Value:      "",
		Payload:    body,
		Properties: properties,
	}

	// async send
	if async {
		producer.SendAsync(
			ctx,
			msg,
			func(mi pulsar.MessageID, pm *pulsar.ProducerMessage, e error) {
				currentStats.Messages = 1
				if e != nil {
					currentStats.Errors++
				}
				if errStats := ReportPubishMetrics(ctx, currentStats); errStats != nil {
					log.Printf("could not report async publish metrics: %v", errStats)
				}
			},
		)

		return nil
	}

	_, err := producer.Send(ctx, msg)
	if err != nil {
		currentStats.Errors++
	}

	if errStats := ReportPubishMetrics(ctx, currentStats); errStats != nil {
		// Log the error instead of fatally stopping the test
		log.Printf("could not report sync publish metrics: %v", errStats)
	}
	return err
}

func ReportPubishMetrics(ctx context.Context, currentStats PublisherStats) error {
	state := lib.GetState(ctx)
	if state == nil {
		return errNilStateOfStats
	}

	tags := make(map[string]string)
	tags["producer_name"] = currentStats.ProducerName
	tags["topic"] = currentStats.Topic

	now := time.Now()

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: PublishMessages,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Messages),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: PublishErrors,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Errors),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: PublishBytes,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Bytes),
	})
	return nil
}
