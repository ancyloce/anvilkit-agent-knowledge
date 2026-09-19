package forwarder_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ancyloce/anvilkit-agent-knowledge/forwarder/internal/forwarder"
)

// knowledgeDir is the Knowledge package beside this module (the Node
// producer whose rows the sidecar reads).
func knowledgeDir(t *testing.T) string {
	t.Helper()
	wd, _ := os.Getwd()
	dir := filepath.Clean(filepath.Join(wd, "..", "..", ".."))
	if _, err := os.Stat(filepath.Join(dir, "dist", "localcheck.js")); err != nil {
		t.Skipf("UNEXECUTED: the Knowledge package is not built (%s/dist/localcheck.js): %v", dir, err)
	}
	return dir
}

func migrationsDir(t *testing.T) string {
	t.Helper()
	if d := os.Getenv("ANVILKIT_KNOWLEDGE_MIGRATIONS_DIR"); d != "" {
		return d
	}
	wd, _ := os.Getwd()
	for d := wd; d != filepath.Dir(d); d = filepath.Dir(d) {
		c := filepath.Join(d, "jobs", "migration", "internal", "migrate", "sql", "knowledge")
		if _, err := os.Stat(filepath.Join(c, "00001_init.sql")); err == nil {
			return c
		}
	}
	t.Skip("UNEXECUTED: anvilkit_knowledge migrations not found")
	return ""
}

// TestNodeRowsForwardedToJetStream: the Node owner writes the outbox
// through its own entry point; the pinned Go adapter reads the row and the
// forwarder publishes the unwrapped envelope to its subject on a real
// JetStream with Nats-Msg-Id = eventId (the interoperability proof of the
// physical schema, DD-09 §2).
func TestNodeRowsForwardedToJetStream(t *testing.T) {
	if os.Getenv("ANVILKIT_SKIP_DOCKER_TESTS") != "" {
		t.Skip("ANVILKIT_SKIP_DOCKER_TESTS set")
	}
	knowledge := knowledgeDir(t)
	migrations := migrationsDir(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	pg, err := postgres.Run(ctx, "postgres:17-alpine", postgres.WithUsername("postgres"), postgres.WithPassword("postgres"), postgres.WithDatabase("postgres"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, pg)
	adminDSN, _ := pg.ConnectionString(ctx, "sslmode=disable")
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{
		"CREATE ROLE anvilkit_knowledge_app LOGIN PASSWORD 'app'", "CREATE ROLE anvilkit_knowledge_migrator LOGIN PASSWORD 'migrator'",
		"CREATE ROLE anvilkit_knowledge_relay LOGIN PASSWORD 'relay'", "CREATE ROLE anvilkit_knowledge_forwarder LOGIN PASSWORD 'forwarder'",
		"CREATE DATABASE anvilkit_knowledge OWNER anvilkit_knowledge_migrator",
	} {
		if _, err := admin.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	admin.Close(ctx)
	host, _ := pg.Host(ctx)
	port, _ := pg.MappedPort(ctx, "5432/tcp")
	dsn := func(role, pw string) string {
		return fmt.Sprintf("postgres://%s:%s@%s:%s/anvilkit_knowledge?sslmode=disable", role, pw, host, port.Port())
	}
	mig, err := pgx.Connect(ctx, dsn("anvilkit_knowledge_migrator", "migrator"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mig.Exec(ctx, "REVOKE CREATE ON SCHEMA public FROM PUBLIC"); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(migrations, "*.sql"))
	for _, f := range files {
		raw, _ := os.ReadFile(f)
		up := strings.Replace(strings.SplitN(string(raw), "-- +goose Down", 2)[0], "-- +goose Up", "", 1)
		if _, err := mig.Exec(ctx, up); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
	}
	if _, err := mig.Exec(ctx, `INSERT INTO sources (source_id, tenant_id, kind, locator, command_id, request_digest) VALUES ('src_1', 'tenant_a', 'document', 'file://x', 'cmd_1', 'sha256:0000000000000000000000000000000000000000000000000000000000000000')`); err != nil {
		t.Fatal(err)
	}
	mig.Close(ctx)

	natsC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: "nats:2.12.4-alpine", Cmd: []string{"-js"}, ExposedPorts: []string{"4222/tcp"}, WaitingFor: wait.ForLog("Server is ready"),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, natsC)
	natsHost, _ := natsC.Host(ctx)
	natsPort, _ := natsC.MappedPort(ctx, "4222/tcp")
	natsURL := fmt.Sprintf("nats://%s:%s", natsHost, natsPort.Port())
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "ANVILKIT_KNOWLEDGE", Subjects: []string{"anvilkit.knowledge.>"}, Duplicates: 2 * time.Minute}); err != nil {
		t.Fatal(err)
	}

	// The Node owner writes a request and its outbox row in one transaction.
	cmd := exec.CommandContext(ctx, "node", filepath.Join(knowledge, "dist", "localcheck.js"), "request", "--source", "src_1", "--bytes", "interop")
	cmd.Env = append(os.Environ(), "ANVILKIT_KNOWLEDGE_CONFIG="+filepath.Join(knowledge, "config.yaml"), "ANVILKIT_KNOWLEDGE_DATABASE_URL="+dsn("anvilkit_knowledge_app", "app"))
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Fatalf("node localcheck: %v: %s", err, ee.Stderr)
		}
		t.Fatal(err)
	}
	var created struct {
		TaskID      string `json:"taskId"`
		InputDigest string `json:"inputDigest"`
	}
	if err := json.Unmarshal(out, &created); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	gen, err := forwarder.Load([]string{"ANVILKIT_FORWARDER_DATABASE_URL=" + dsn("anvilkit_knowledge_forwarder", "forwarder"), "ANVILKIT_FORWARDER_NATS_URL=" + natsURL, "ANVILKIT_FORWARDER_POLL_INTERVAL=100ms", "ANVILKIT_FORWARDER_CLOSE_TIMEOUT=5s"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	metrics := forwarder.NewMetrics(prometheus.NewRegistry())
	rt, err := forwarder.Build(ctx, gen, metrics)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	gens := forwarder.NewGenerations(nil, rt, metrics, log)
	defer gens.Shutdown()

	consumer, err := js.CreateOrUpdateConsumer(ctx, "ANVILKIT_KNOWLEDGE", jetstream.ConsumerConfig{Durable: "test-relay", FilterSubject: "anvilkit.knowledge.background.requested", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := consumer.Fetch(1, jetstream.FetchMaxWait(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var got jetstream.Msg
	for m := range msgs.Messages() {
		got = m
	}
	if got == nil {
		t.Fatalf("no message forwarded: %v", msgs.Error())
	}
	var env struct {
		EventID   string `json:"eventId"`
		EventType string `json:"eventType"`
		Producer  string `json:"producer"`
		Subject   string `json:"subject"`
		TenantID  string `json:"tenantId"`
		Payload   struct {
			TaskID      string `json:"taskId"`
			TaskKind    string `json:"taskKind"`
			InputDigest string `json:"inputDigest"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(got.Data(), &env); err != nil {
		t.Fatalf("%v: %s", err, got.Data())
	}
	if env.EventType != "background.requested" || env.Producer != "anvilkit-agent-knowledge" || env.Subject != got.Subject() || env.TenantID != "tenant_a" || env.Payload.TaskID != created.TaskID || env.Payload.TaskKind != "local-check" || env.Payload.InputDigest != created.InputDigest {
		t.Fatalf("envelope: %+v (subject %s)", env, got.Subject())
	}
	if got.Headers().Get(nats.MsgIdHdr) != env.EventID || got.Headers().Get("_watermill_message_uuid") != env.EventID {
		t.Fatalf("headers: %v", got.Headers())
	}
	if got.Headers().Get("anvilkit_event_type") != "background.requested" || got.Headers().Get("anvilkit_tenant_id") != "tenant_a" {
		t.Fatalf("metadata headers: %v", got.Headers())
	}
	_ = base64.StdEncoding
	if err := got.Ack(); err != nil {
		t.Fatal(err)
	}
	// The offsets row belongs to the forwarder identity and advanced past the row.
	fwdConn, err := pgx.Connect(ctx, dsn("anvilkit_knowledge_forwarder", "forwarder"))
	if err != nil {
		t.Fatal(err)
	}
	defer fwdConn.Close(ctx)
	var acked int64
	deadline := time.Now().Add(10 * time.Second)
	for {
		_ = fwdConn.QueryRow(ctx, "SELECT COALESCE(offset_acked, 0) FROM outbox_offsets WHERE consumer_group = $1", gen.Config.ConsumerGroup).Scan(&acked)
		if acked >= 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if acked < 1 {
		t.Fatalf("offsets not acknowledged: %d", acked)
	}
	if _, err := fwdConn.Exec(ctx, "SELECT count(*) FROM background_requests"); err == nil {
		t.Fatal("the forwarder identity must not read domain tables")
	}
}
