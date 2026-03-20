//go:generate ../../../tools/readme_config_includer/generator
package nats

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal/choice"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

type NATS struct {
	Servers        []string      `toml:"servers"`
	Secure         bool          `toml:"secure"`
	Name           string        `toml:"name"`
	Username       config.Secret `toml:"username"`
	Password       config.Secret `toml:"password"`
	Credentials    string        `toml:"credentials"`
	Subject        string        `toml:"subject"`
	Jetstream      *StreamConfig `toml:"jetstream"`
	TracePropagation bool   `toml:"trace_propagation"`
	OtelEndpoint     string `toml:"otel_endpoint"`
	tls.ClientConfig

	Log telegraf.Logger `toml:"-"`

	conn                  *nats.Conn
	jetstreamClient       jetstream.JetStream
	jetstreamStreamConfig *jetstream.StreamConfig
	serializer            telegraf.Serializer
	tracerProvider        *sdktrace.TracerProvider
}

// StreamConfig is the configuration for creating stream
// Almost a mirror of https://pkg.go.dev/github.com/nats-io/nats.go/jetstream#StreamConfig but with TOML tags
type StreamConfig struct {
	Name                    string                            `toml:"name"`
	Description             string                            `toml:"description"`
	Subjects                []string                          `toml:"subjects"`
	Retention               string                            `toml:"retention"`
	MaxConsumers            int                               `toml:"max_consumers"`
	MaxMsgs                 int64                             `toml:"max_msgs"`
	MaxBytes                int64                             `toml:"max_bytes"`
	Discard                 string                            `toml:"discard"`
	DiscardNewPerSubject    bool                              `toml:"discard_new_per_subject"`
	MaxAge                  config.Duration                   `toml:"max_age"`
	MaxMsgsPerSubject       int64                             `toml:"max_msgs_per_subject"`
	MaxMsgSize              int32                             `toml:"max_msg_size"`
	Storage                 string                            `toml:"storage"`
	Replicas                int                               `toml:"num_replicas"`
	NoAck                   bool                              `toml:"no_ack"`
	Template                string                            `toml:"template_owner"`
	Duplicates              config.Duration                   `toml:"duplicate_window"`
	Placement               *jetstream.Placement              `toml:"placement"`
	Mirror                  *jetstream.StreamSource           `toml:"mirror"`
	Sources                 []*jetstream.StreamSource         `toml:"sources"`
	Sealed                  bool                              `toml:"sealed"`
	DenyDelete              bool                              `toml:"deny_delete"`
	DenyPurge               bool                              `toml:"deny_purge"`
	AllowRollup             bool                              `toml:"allow_rollup_hdrs"`
	Compression             string                            `toml:"compression"`
	FirstSeq                uint64                            `toml:"first_seq"`
	SubjectTransform        *jetstream.SubjectTransformConfig `toml:"subject_transform"`
	RePublish               *jetstream.RePublish              `toml:"republish"`
	AllowDirect             bool                              `toml:"allow_direct"`
	MirrorDirect            bool                              `toml:"mirror_direct"`
	ConsumerLimits          jetstream.StreamConsumerLimits    `toml:"consumer_limits"`
	Metadata                map[string]string                 `toml:"metadata"`
	ExistingStream          bool                              `toml:"existing_stream"`
	AsyncPublish            bool                              `toml:"async_publish"`
	AsyncAckTimeout         string                            `toml:"async_ack_timeout"`
	AsyncMaxAckPending      int                               `toml:"async_max_ack_pending"`
	AsyncAckTimeoutDuration time.Duration
}

func (*NATS) SampleConfig() string {
	return sampleConfig
}

func (n *NATS) SetSerializer(serializer telegraf.Serializer) {
	n.serializer = serializer
}

func (n *NATS) Connect() error {
	var err error

	opts := []nats.Option{
		nats.MaxReconnects(-1),
	}

	// override authentication, if any was specified
	if !n.Username.Empty() && !n.Password.Empty() {
		username, err := n.Username.Get()
		if err != nil {
			return fmt.Errorf("getting username failed: %w", err)
		}
		password, err := n.Password.Get()
		if err != nil {
			username.Destroy()
			return fmt.Errorf("getting password failed: %w", err)
		}
		opts = append(opts, nats.UserInfo(username.String(), password.String()))
		username.Destroy()
		password.Destroy()
	}

	if n.Credentials != "" {
		opts = append(opts, nats.UserCredentials(n.Credentials))
	}

	if n.Name != "" {
		opts = append(opts, nats.Name(n.Name))
	}

	if n.Secure {
		tlsConfig, err := n.ClientConfig.TLSConfig()
		if err != nil {
			return err
		}

		opts = append(opts, nats.Secure(tlsConfig))
	}

	// try and connect
	n.conn, err = nats.Connect(strings.Join(n.Servers, ","), opts...)
	if err != nil {
		return err
	}

	if n.Jetstream != nil {
		n.jetstreamClient, err = jetstream.New(n.conn, jetstream.WithPublishAsyncMaxPending(n.Jetstream.AsyncMaxAckPending))
		if err != nil {
			return fmt.Errorf("failed to connect to jetstream: %w", err)
		}
		switch n.Jetstream.ExistingStream {
		case true:
			_, err = n.jetstreamClient.Stream(context.Background(), n.Jetstream.Name)
			if err != nil {
				return fmt.Errorf("failed to connect to stream: %w", err)
			}
		default:
			_, err = n.jetstreamClient.CreateOrUpdateStream(context.Background(), *n.jetstreamStreamConfig)
			if err != nil {
				return fmt.Errorf("failed to create or update stream: %w", err)
			}
		}
		n.Log.Infof("Successfully connected to stream (%s)", n.Jetstream.Name)
	}
	return nil
}

func (n *NATS) getJetstreamConfig() (*jetstream.StreamConfig, error) {
	var retention jetstream.RetentionPolicy
	switch n.Jetstream.Retention {
	case "", "limits":
		retention = jetstream.LimitsPolicy
	case "interest":
		retention = jetstream.InterestPolicy
	case "workqueue":
		retention = jetstream.WorkQueuePolicy
	default:
		return nil, fmt.Errorf("invalid 'retention' setting %q", n.Jetstream.Retention)
	}

	var discard jetstream.DiscardPolicy
	switch n.Jetstream.Discard {
	case "", "old":
		discard = jetstream.DiscardOld
	case "new":
		discard = jetstream.DiscardNew
	default:
		return nil, fmt.Errorf("invalid 'discard' setting %q", n.Jetstream.Discard)
	}

	var storage jetstream.StorageType
	switch n.Jetstream.Storage {
	case "memory":
		storage = jetstream.MemoryStorage
	case "", "file":
		storage = jetstream.FileStorage
	default:
		return nil, fmt.Errorf("invalid 'storage' setting %q", n.Jetstream.Storage)
	}

	var compression jetstream.StoreCompression
	switch n.Jetstream.Compression {
	case "s2":
		compression = jetstream.S2Compression
	case "", "none":
		compression = jetstream.NoCompression
	default:
		return nil, fmt.Errorf("invalid 'compression' setting %q", n.Jetstream.Compression)
	}

	streamConfig := &jetstream.StreamConfig{
		Name:                 n.Jetstream.Name,
		Description:          n.Jetstream.Description,
		Subjects:             n.Jetstream.Subjects,
		Retention:            retention,
		MaxConsumers:         n.Jetstream.MaxConsumers,
		MaxMsgs:              n.Jetstream.MaxMsgs,
		MaxBytes:             n.Jetstream.MaxBytes,
		Discard:              discard,
		DiscardNewPerSubject: n.Jetstream.DiscardNewPerSubject,
		MaxAge:               time.Duration(n.Jetstream.MaxAge),
		MaxMsgsPerSubject:    n.Jetstream.MaxMsgsPerSubject,
		MaxMsgSize:           n.Jetstream.MaxMsgSize,
		Storage:              storage,
		Replicas:             n.Jetstream.Replicas,
		NoAck:                n.Jetstream.NoAck,
		Template:             n.Jetstream.Template,
		Duplicates:           time.Duration(n.Jetstream.Duplicates),
		Placement:            n.Jetstream.Placement,
		Mirror:               n.Jetstream.Mirror,
		Sources:              n.Jetstream.Sources,
		Sealed:               n.Jetstream.Sealed,
		DenyDelete:           n.Jetstream.DenyDelete,
		DenyPurge:            n.Jetstream.DenyPurge,
		AllowRollup:          n.Jetstream.AllowRollup,
		Compression:          compression,
		FirstSeq:             n.Jetstream.FirstSeq,
		SubjectTransform:     n.Jetstream.SubjectTransform,
		RePublish:            n.Jetstream.RePublish,
		AllowDirect:          n.Jetstream.AllowDirect,
		MirrorDirect:         n.Jetstream.MirrorDirect,
		ConsumerLimits:       n.Jetstream.ConsumerLimits,
		Metadata:             n.Jetstream.Metadata,
	}
	return streamConfig, nil
}

func (n *NATS) Init() error {
	if n.Jetstream != nil {
		if strings.TrimSpace(n.Jetstream.Name) == "" {
			return errors.New("stream cannot be empty")
		}

		if len(n.Jetstream.Subjects) == 0 {
			n.Jetstream.Subjects = []string{n.Subject}
		}
		// If the overall-subject is already present anywhere in the Jetstream subject we go from there,
		// otherwise we should append the overall-subject as the last element.
		if !choice.Contains(n.Subject, n.Jetstream.Subjects) {
			n.Jetstream.Subjects = append(n.Jetstream.Subjects, n.Subject)
		}

		var err error
		if n.Jetstream.AsyncAckTimeout == "" {
			n.Jetstream.AsyncAckTimeoutDuration = 5 * time.Second
		} else {
			n.Jetstream.AsyncAckTimeoutDuration, err = time.ParseDuration(n.Jetstream.AsyncAckTimeout)
			if err != nil {
				return fmt.Errorf("failed to parse async_ack_timeout config: %w", err)
			}
		}

		n.jetstreamStreamConfig, err = n.getJetstreamConfig()
		if err != nil {
			return fmt.Errorf("failed to parse jetstream config: %w", err)
		}
	}

	if n.TracePropagation {
		if n.OtelEndpoint == "" {
			return errors.New("otel_endpoint must be set when trace_propagation is enabled")
		}
		traceExp, err := otlptracegrpc.New(
			context.Background(),
			otlptracegrpc.WithEndpoint(n.OtelEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return fmt.Errorf("failed to create OTLP trace exporter: %w", err)
		}
		n.tracerProvider = sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp))
		otel.SetTracerProvider(n.tracerProvider)
		otel.SetTextMapPropagator(propagation.TraceContext{})
	}

	return nil
}

func (n *NATS) Close() error {
	if n.tracerProvider != nil {
		if err := n.tracerProvider.Shutdown(context.Background()); err != nil {
			n.Log.Warnf("Failed to shutdown tracer provider: %v", err)
		}
	}
	n.conn.Close()
	return nil
}

// natsHeaderCarrier adapts nats.Header to satisfy the propagation.TextMapCarrier interface.
type natsHeaderCarrier nats.Header

func (c natsHeaderCarrier) Get(key string) string {
	return nats.Header(c).Get(key)
}

func (c natsHeaderCarrier) Set(key, value string) {
	nats.Header(c).Set(key, value)
}

func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// newTraceparentMsg creates a nats.Msg with a W3C traceparent header injected
// from the given context.
func newTraceparentMsg(ctx context.Context, subject string, data []byte) *nats.Msg {
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	otel.GetTextMapPropagator().Inject(ctx, natsHeaderCarrier(msg.Header))
	return msg
}

func (n *NATS) Write(metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	var pafs []jetstream.PubAckFuture
	if n.Jetstream != nil && n.Jetstream.AsyncPublish {
		pafs = make([]jetstream.PubAckFuture, len(metrics))
	}

	for i, metric := range metrics {
		buf, err := n.serializer.Serialize(metric)
		if err != nil {
			n.Log.Debugf("Could not serialize metric: %v", err)
			continue
		}
		if n.TracePropagation {
			tracer := otel.Tracer("telegraf/outputs/nats")
			ctx, span := tracer.Start(context.Background(), n.Subject, trace.WithSpanKind(trace.SpanKindProducer))
			msg := newTraceparentMsg(ctx, n.Subject, buf)
			if n.Jetstream != nil {
				if n.Jetstream.AsyncPublish {
					pafs[i], err = n.jetstreamClient.PublishMsgAsync(msg, jetstream.WithExpectStream(n.Jetstream.Name))
				} else {
					_, err = n.jetstreamClient.PublishMsg(ctx, msg, jetstream.WithExpectStream(n.Jetstream.Name))
				}
			} else {
				err = n.conn.PublishMsg(msg)
			}
			span.End()
		} else {
			if n.Jetstream != nil {
				if n.Jetstream.AsyncPublish {
					pafs[i], err = n.jetstreamClient.PublishAsync(n.Subject, buf, jetstream.WithExpectStream(n.Jetstream.Name))
				} else {
					_, err = n.jetstreamClient.Publish(context.Background(), n.Subject, buf, jetstream.WithExpectStream(n.Jetstream.Name))
				}
			} else {
				err = n.conn.Publish(n.Subject, buf)
			}
		}
		if err != nil {
			return fmt.Errorf("failed to send NATS message: %w", err)
		}
	}

	if pafs != nil {
		// Check Ack from async publish
		for i := range pafs {
			select {
			case <-pafs[i].Ok():
				continue
			case err := <-pafs[i].Err():
				return fmt.Errorf("publish acknowledgement is an error: %w (retrying)", err)
			case <-time.After(n.Jetstream.AsyncAckTimeoutDuration):
				return errors.New("Async publish complete, but futures did not return")
			}
		}
	}
	return nil
}

func init() {
	outputs.Add("nats", func() telegraf.Output {
		return &NATS{}
	})
}
