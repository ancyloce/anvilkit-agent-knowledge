package forwarder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmjs "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
)

// The forwarder topic and the physical table names of the adapter
// (migration 00002 of anvilkit_knowledge).
const (
	ForwarderTopic = "outbox"
	MessagesTable  = `"outbox"`
	OffsetsTable   = `"outbox_offsets"`
)

// Schema is the adapter's PostgreSQL schema with the owner's table names.
func Schema(batchSize int) sql.DefaultPostgreSQLSchema {
	return sql.DefaultPostgreSQLSchema{GenerateMessagesTableName: func(string) string { return MessagesTable }, SubscribeBatchSize: batchSize}
}

// Offsets is the adapter's offsets schema with the owner's table name.
func Offsets() sql.DefaultPostgreSQLOffsetsAdapter {
	return sql.DefaultPostgreSQLOffsetsAdapter{GenerateMessagesOffsetsTableName: func(string) string { return OffsetsTable }}
}

// Metrics are the sidecar's signals.
type Metrics struct {
	Forwarded        prometheus.Counter
	Failures         prometheus.Counter
	ConfigGeneration prometheus.Gauge
	ConfigRejections prometheus.Counter
	DrainSeconds     prometheus.Gauge
	ForcedStop       prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Forwarded:        prometheus.NewCounter(prometheus.CounterOpts{Name: "anvilkit_knowledge_outbox_forwarded_total", Help: "Outbox messages published to JetStream."}),
		Failures:         prometheus.NewCounter(prometheus.CounterOpts{Name: "anvilkit_knowledge_outbox_forward_failures_total", Help: "Forwarder publish failures (the message is retried)."}),
		ConfigGeneration: prometheus.NewGauge(prometheus.GaugeOpts{Name: "anvilkit_knowledge_forwarder_config_generation", Help: "Number of the active configuration generation."}),
		ConfigRejections: prometheus.NewCounter(prometheus.CounterOpts{Name: "anvilkit_knowledge_forwarder_config_rejections_total", Help: "Candidate generations rejected."}),
		DrainSeconds:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "anvilkit_knowledge_forwarder_shutdown_drain_seconds", Help: "Seconds the last shutdown spent draining."}),
		ForcedStop:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "anvilkit_knowledge_forwarder_shutdown_forced", Help: "1 when the last shutdown had to force-stop."}),
	}
	reg.MustRegister(m.Forwarded, m.Failures, m.ConfigGeneration, m.ConfigRejections, m.DrainSeconds, m.ForcedStop)
	return m
}

// Runtime is one generation's clients: its pool, NATS connection and
// forwarder, built off-path and probed before they run.
type Runtime struct {
	gen    Generation
	pool   *pgxpool.Pool
	nc     *nats.Conn
	sub    *sql.Subscriber
	fwd    *forwarder.Forwarder
	cancel context.CancelFunc
	done   chan struct{}
}

// Build constructs and probes the runtime of a generation.
func Build(ctx context.Context, gen Generation, metrics *Metrics) (rt *Runtime, err error) {
	c := gen.Config
	poolCfg, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("database url: %w", err)
	}
	poolCfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			pool.Close()
		}
	}()
	probe, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err = pool.Ping(probe); err != nil {
		return nil, fmt.Errorf("database probe: %w", err)
	}
	// The identity's privileges are part of the probe: messages readable,
	// offsets writable, the domain tables out of reach.
	if _, err = pool.Exec(probe, `SELECT 1 FROM outbox LIMIT 1`); err != nil {
		return nil, fmt.Errorf("outbox read probe: %w", err)
	}
	nc, err := nats.Connect(c.NATSURL, nats.Name("anvilkit-knowledge-forwarder"), nats.Timeout(c.PublishTimeout), nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	defer func() {
		if err != nil {
			nc.Close()
		}
	}()
	if _, err = jetstream.New(nc); err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	ack := c.AckDeadline
	sub, err := sql.NewSubscriber(sql.BeginnerFromPgx(pool), sql.SubscriberConfig{
		ConsumerGroup: c.ConsumerGroup, AckDeadline: &ack, PollInterval: c.PollInterval, ResendInterval: c.ResendInterval, RetryInterval: c.PollInterval,
		SchemaAdapter: Schema(c.BatchSize), OffsetsAdapter: Offsets(), InitializeSchema: false,
	}, watermill.NopLogger{})
	if err != nil {
		return nil, fmt.Errorf("outbox subscriber: %w", err)
	}
	pub, err := wmjs.NewPublisher(wmjs.PublisherConfig{
		Conn: nc, Logger: watermill.NopLogger{}, TrackMessageID: true,
		// The publisher reads only the stream name of this configurator, as
		// the publish subject (watermill-nats v2.2.0); the streams that
		// capture the subjects are deployment inputs.
		ConfigureStream: func(topic string) jetstream.StreamConfig { return jetstream.StreamConfig{Name: topic} },
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream publisher: %w", err)
	}
	fwd, err := forwarder.NewForwarder(sub, &counting{Publisher: pub, m: metrics}, watermill.NopLogger{}, forwarder.Config{ForwarderTopic: ForwarderTopic, HandlerName: "anvilkit-knowledge-outbox-forwarder", CloseTimeout: c.CloseTimeout})
	if err != nil {
		return nil, fmt.Errorf("forwarder: %w", err)
	}
	return &Runtime{gen: gen, pool: pool, nc: nc, sub: sub, fwd: fwd}, nil
}

// Start runs the forwarder in the background.
func (rt *Runtime) Start(log *slog.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	rt.cancel = cancel
	rt.done = make(chan struct{})
	go func() {
		defer close(rt.done)
		if err := rt.fwd.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("forwarder stopped", "generation", rt.gen.Number, "error", err)
		}
	}()
}

// Running is closed once the forwarder consumes.
func (rt *Runtime) Running() chan struct{} { return rt.fwd.Running() }

// Retire stops the forwarder within the limit and closes the connections.
func (rt *Runtime) Retire(limit time.Duration) (forced bool) {
	if rt.cancel != nil {
		rt.cancel()
	}
	_ = rt.fwd.Close()
	if rt.done != nil {
		select {
		case <-rt.done:
		case <-time.After(limit):
			forced = true
		}
	}
	_ = rt.sub.Close()
	rt.nc.Close()
	closed := make(chan struct{})
	go func() { rt.pool.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(limit):
		forced = true
	}
	return forced
}

type counting struct {
	message.Publisher
	m *Metrics
}

func (c *counting) Publish(topic string, msgs ...*message.Message) error {
	if err := c.Publisher.Publish(topic, msgs...); err != nil {
		c.m.Failures.Inc()
		return err
	}
	c.m.Forwarded.Add(float64(len(msgs)))
	return nil
}

// Generations publishes and retires runtimes as the inputs change.
type Generations struct {
	environ []string
	metrics *Metrics
	log     *slog.Logger
	mu      sync.Mutex
	active  *Runtime
	number  uint64
}

func NewGenerations(environ []string, first *Runtime, metrics *Metrics, log *slog.Logger) *Generations {
	g := &Generations{environ: environ, metrics: metrics, log: log, number: first.gen.Number}
	g.activate(first)
	return g
}

func (g *Generations) activate(rt *Runtime) {
	g.mu.Lock()
	previous := g.active
	g.active = rt
	g.mu.Unlock()
	rt.Start(g.log)
	g.metrics.ConfigGeneration.Set(float64(rt.gen.Number))
	g.log.Info("configuration generation active", "generation", rt.gen.Number, "digest", rt.gen.Digest, "secretRevision", rt.gen.SecretRevision, "consumerGroup", rt.gen.Config.ConsumerGroup)
	if previous != nil {
		started := time.Now()
		forced := previous.Retire(rt.gen.Config.CloseTimeout)
		g.log.Info("previous generation drained", "generation", previous.gen.Number, "seconds", time.Since(started).Seconds(), "forced", forced)
	}
}

// Reload builds a candidate from the current inputs when they changed.
func (g *Generations) Reload(ctx context.Context) error {
	g.mu.Lock()
	active := g.active.gen
	g.mu.Unlock()
	candidate, err := Load(g.environ, g.number+1)
	if err != nil {
		g.metrics.ConfigRejections.Inc()
		return err
	}
	if candidate.Inputs() == active.Inputs() {
		return nil
	}
	rt, err := Build(ctx, candidate, g.metrics)
	if err != nil {
		g.metrics.ConfigRejections.Inc()
		return fmt.Errorf("generation %d rejected: %w", candidate.Number, err)
	}
	g.number = candidate.Number
	g.activate(rt)
	return nil
}

// Watch re-reads the inputs until ctx ends.
func (g *Generations) Watch(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := g.Reload(ctx); err != nil {
				g.log.Warn("configuration candidate rejected; the active generation stays", "error", err)
			}
		}
	}
}

// Shutdown retires the active generation.
func (g *Generations) Shutdown() (forced bool) {
	g.mu.Lock()
	rt := g.active
	g.active = nil
	g.mu.Unlock()
	if rt == nil {
		return false
	}
	return rt.Retire(rt.gen.Config.CloseTimeout)
}
