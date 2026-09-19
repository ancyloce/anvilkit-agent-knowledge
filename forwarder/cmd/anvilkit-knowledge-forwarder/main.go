// anvilkit-knowledge-forwarder: the Knowledge outbox forwarder sidecar
// (DD-09 §2). Fx orders the lifecycle: the probe/metrics listener first,
// the first generation's runtime probed before anything runs, the
// generation watcher, then readiness; on stop readiness is withdrawn, the
// forwarder drains within its close timeout (forced afterwards), the
// watcher stops and the connections close, the probe listener last.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/ancyloce/anvilkit-agent-knowledge/forwarder/internal/forwarder"
)

func main() {
	app := fx.New(Module())
	if err := app.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "forwarder:", err)
		os.Exit(1)
	}
	startCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	if err := app.Start(startCtx); err != nil {
		cancel()
		fmt.Fprintln(os.Stderr, "forwarder start:", err)
		os.Exit(1)
	}
	cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelStop()
	if err := app.Stop(stopCtx); err != nil {
		fmt.Fprintln(os.Stderr, "forwarder stop:", err)
		os.Exit(1)
	}
}

// Module is the sidecar's assembly.
func Module() fx.Option {
	return fx.Options(
		fx.StopTimeout(4*time.Minute),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: log} }),
		fx.Provide(
			func() (forwarder.Generation, error) { return forwarder.Load(os.Environ(), 1) },
			func() *slog.Logger {
				return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
			},
			func() *prometheus.Registry { return prometheus.NewRegistry() },
			func(reg *prometheus.Registry) *forwarder.Metrics { return forwarder.NewMetrics(reg) },
		),
		fx.Invoke(run),
	)
}

func run(lc fx.Lifecycle, gen forwarder.Generation, metrics *forwarder.Metrics, reg *prometheus.Registry, log *slog.Logger) {
	var ready atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	health := &http.Server{Addr: gen.Config.HealthListen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	var gens *forwarder.Generations
	loops, cancelLoops := context.WithCancel(context.Background())
	watcherDone := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) (err error) {
			ln, err := net.Listen("tcp", gen.Config.HealthListen)
			if err != nil {
				return fmt.Errorf("health listener: %w", err)
			}
			go func() { _ = health.Serve(ln) }()
			rt, err := forwarder.Build(ctx, gen, metrics)
			if err != nil {
				metrics.ConfigRejections.Inc()
				_ = health.Shutdown(context.Background())
				return fmt.Errorf("configuration generation %d rejected: %w", gen.Number, err)
			}
			gens = forwarder.NewGenerations(os.Environ(), rt, metrics, log)
			select {
			case <-rt.Running():
			case <-time.After(30 * time.Second):
				_ = health.Shutdown(context.Background())
				gens.Shutdown()
				return fmt.Errorf("forwarder did not start consuming within 30s")
			}
			go func() { defer close(watcherDone); gens.Watch(loops, gen.Config.ReloadInterval) }()
			ready.Store(true)
			log.Info("knowledge forwarder running", "consumerGroup", gen.Config.ConsumerGroup, "health", gen.Config.HealthListen)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			begin := time.Now()
			ready.Store(false)
			cancelLoops()
			forced := false
			select {
			case <-watcherDone:
			case <-ctx.Done():
				forced = true
			}
			if gens != nil {
				forced = gens.Shutdown() || forced
			}
			metrics.DrainSeconds.Set(time.Since(begin).Seconds())
			if forced {
				metrics.ForcedStop.Set(1)
			}
			log.Info("knowledge forwarder stopped", "drainSeconds", time.Since(begin).Seconds(), "forced", forced)
			return health.Shutdown(ctx)
		},
	})
}
