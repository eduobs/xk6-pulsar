package xpulsar

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/sirupsen/logrus"

	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
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

type PulsarMetrics struct {
	PublishMessages *metrics.Counter
	PublishBytes    *metrics.Counter
	PublishErrors   *metrics.Counter
}

type PubSub struct {
	vu      modules.VU
	metrics PulsarMetrics
}

func init() {
	modules.Register("k6/x/pulsar", New)
}

func New() *PubSub { return &PubSub{} }

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

func (p *PubSub) XModuleInstance(vu modules.VU) modules.Instance {
	registry := vu.InitEnv().Registry
	m, err := registerMetrics(registry)
	if err != nil {
		common.Throw(vu.Runtime(), err)
	}

	return &PubSub{
		vu:      vu,
		metrics: m,
	}
}

func (p *PubSub) Exports() modules.Exports {
	return modules.Exports{
		Named: map[string]interface{}{
			"createClient":   p.CreateClient,
			"createProducer": p.CreateProducer,
			"publish":        p.Publish,
			"closeClient":    p.CloseClient,
			"closeProducer":  p.CloseProducer,
		},
	}
}

func registerMetrics(registry *metrics.Registry) (PulsarMetrics, error) {
	var err error
	m := PulsarMetrics{}

	m.PublishMessages, err = registry.NewMetric("pulsar.publish.message.count", metrics.Counter)
	if err != nil {
		return m, err
	}
	m.PublishBytes, err = registry.NewMetric("pulsar.publish.message.bytes", metrics.Counter, metrics.Data)
	if err != nil {
		return m, err
	}
	m.PublishErrors, err = registry.NewMetric("pulsar.publish.error.count", metrics.Counter)
	if err != nil {
		return m, err
	}

	return m, nil
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
	state := p.vu.State()
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
				if errStats := p.ReportPubishMetrics(ctx, currentStats); errStats != nil {
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

	if errStats := p.ReportPubishMetrics(ctx, currentStats); errStats != nil {
		// Log the error instead of fatally stopping the test
		log.Printf("could not report sync publish metrics: %v", errStats)
	}
	return err
}

func (p *PubSub) ReportPubishMetrics(ctx context.Context, currentStats PublisherStats) error {
	state := p.vu.State()
	if state == nil {
		return errNilStateOfStats
	}

	tags := metrics.NewTags(
		"producer_name", currentStats.ProducerName,
		"topic", currentStats.Topic,
	)

	p.metrics.PublishMessages.WithTags(tags).Add(float64(currentStats.Messages))
	p.metrics.PublishErrors.WithTags(tags).Add(float64(currentStats.Errors))
	p.metrics.PublishBytes.WithTags(tags).Add(float64(currentStats.Bytes))

	metrics.PushIfNotDone(ctx, state.Samples)
	return nil
}
