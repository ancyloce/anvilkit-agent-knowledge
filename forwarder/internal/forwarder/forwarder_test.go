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
	"github.com/nats-io/nkeys"
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
		// P17: migration 00005 lets the PostgresStore vendor identity create its schema.
		"CREATE ROLE anvilkit_knowledge_store_migrator LOGIN PASSWORD 'store'",
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
	// The owner's configuration needs its workload identity files (P0.1): the
	// environment's, else the development PKI's (deploy/dev/certs.sh).
	if os.Getenv("ANVILKIT_KNOWLEDGE_IDENTITY_CERT_FILE") == "" {
		certs := filepath.Join(knowledge, "..", "..", "..", ".local", "dev", "certs", "anvilkit-agent-knowledge")
		if _, err := os.Stat(filepath.Join(certs, "tls.key")); err != nil {
			t.Skipf("UNEXECUTED: the Node producer needs a Knowledge workload identity (ANVILKIT_KNOWLEDGE_IDENTITY_*_FILE or sh deploy/dev/certs.sh): %v", err)
		}
		cmd.Env = append(cmd.Env, "ANVILKIT_KNOWLEDGE_IDENTITY_CERT_FILE="+filepath.Join(certs, "tls.crt"),
			"ANVILKIT_KNOWLEDGE_IDENTITY_KEY_FILE="+filepath.Join(certs, "tls.key"), "ANVILKIT_KNOWLEDGE_IDENTITY_CA_FILE="+filepath.Join(certs, "ca.crt"))
	}
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

	gen, err := forwarder.Load([]string{"ANVILKIT_FORWARDER_DATABASE_URL=" + dsn("anvilkit_knowledge_forwarder", "forwarder"), "ANVILKIT_FORWARDER_NATS_URL=" + natsURL, "ANVILKIT_FORWARDER_DEVELOPMENT_ENABLED=true", "ANVILKIT_FORWARDER_NATS_TLS_MODE=development", "ANVILKIT_FORWARDER_POLL_INTERVAL=100ms", "ANVILKIT_FORWARDER_CLOSE_TIMEOUT=5s"}, 1)
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

// TestNATSTransportGuard (P0.1): the plaintext JetStream transport needs
// the development mode and the top-level guard; tls needs the bundle.
func TestNATSTransportGuard(t *testing.T) {
	base := []string{"ANVILKIT_FORWARDER_DATABASE_URL=postgres://u:p@127.0.0.1:5432/anvilkit_knowledge", "ANVILKIT_FORWARDER_NATS_URL=nats://127.0.0.1:4222"}
	_, err := forwarder.Load(base, 1)
	if err == nil || !strings.Contains(err.Error(), "NATS_TLS_CA_FILE is required") {
		t.Fatalf("tls by default needs a bundle: %v", err)
	}
	_, err = forwarder.Load(append(base, "ANVILKIT_FORWARDER_NATS_TLS_MODE=development"), 1)
	if err == nil || !strings.Contains(err.Error(), "DEVELOPMENT_ENABLED=true") {
		t.Fatalf("development without the guard: %v", err)
	}
	if _, err = forwarder.Load(append(base, "ANVILKIT_FORWARDER_NATS_TLS_MODE=development", "ANVILKIT_FORWARDER_DEVELOPMENT_ENABLED=true"), 1); err != nil {
		t.Fatalf("development under the guard: %v", err)
	}
	if _, err = forwarder.Load(append(base, "ANVILKIT_FORWARDER_NATS_TLS_MODE=mtls", "ANVILKIT_FORWARDER_NATS_TLS_CA_FILE=/c"), 1); err == nil || !strings.Contains(err.Error(), "NATS_TLS_CERT_FILE") {
		t.Fatalf("mtls needs the certificate: %v", err)
	}
}

// natsCredentials writes a NATS user .creds file and a bare NKey user seed
// (preceded by an empty line) of a fresh user key. The JWT is a placeholder:
// the loader classifies the file, the server verifies the JWT.
func natsCredentials(t *testing.T, dir string) (creds, seed string) {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := kp.Seed()
	if err != nil {
		t.Fatal(err)
	}
	creds, seed = filepath.Join(dir, "user.creds"), filepath.Join(dir, "user.nk")
	content := "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.e30.c2ln\n------END NATS USER JWT------\n\n" +
		"************************* IMPORTANT *************************\nNKEY Seed printed below can be used to sign and prove identity.\n\n" +
		"-----BEGIN USER NKEY SEED-----\n" + string(raw) + "\n------END USER NKEY SEED------\n"
	if err := os.WriteFile(creds, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(seed, []byte("\n"+string(raw)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return creds, seed
}

// TestNATSCredentialsAndServerName (P0.6): the credential is read and
// classified at load (a .creds file or a bare NKey seed, anything else
// rejected without echoing it), required outside development, optional but
// still used under the guard; its rotation changes only the secret
// revision; the TLS server name is taken from the environment.
func TestNATSCredentialsAndServerName(t *testing.T) {
	dir := t.TempDir()
	creds, seed := natsCredentials(t, dir)
	garbage := filepath.Join(dir, "garbage")
	if err := os.WriteFile(garbage, []byte("\n  not-a-credential-SECRETVALUE\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	prod := []string{
		"ANVILKIT_FORWARDER_DATABASE_URL=postgres://u:p@db.example:5432/anvilkit_knowledge?sslmode=verify-full", "ANVILKIT_FORWARDER_NATS_URL=tls://nats.example:4222",
		"ANVILKIT_FORWARDER_NATS_TLS_CA_FILE=/etc/anvilkit/nats/ca.crt", "ANVILKIT_FORWARDER_NATS_TLS_SERVER_NAME=nats.anvilkit-system.svc",
	}
	with := func(base []string, extra ...string) []string { return append(append([]string{}, base...), extra...) }

	g, err := forwarder.Load(with(prod, "ANVILKIT_FORWARDER_NATS_CREDS_FILE="+creds), 1)
	if err != nil {
		t.Fatal(err)
	}
	if c := g.Config; c.NATSCredsFile != creds || c.NATSCredsNKey || c.NATSTLSServerName != "nats.anvilkit-system.svc" || c.NATSTLSMode != "tls" {
		t.Fatalf("%+v", c)
	}
	gs, err := forwarder.Load(with(prod, "ANVILKIT_FORWARDER_NATS_CREDS_FILE="+seed), 1)
	if err != nil {
		t.Fatal(err)
	}
	if !gs.Config.NATSCredsNKey {
		t.Fatalf("a bare seed is an NKey credential: %+v", gs.Config)
	}
	raw, err := os.ReadFile(creds)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(creds, []byte(strings.Replace(string(raw), ".e30.", ".e301.", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	g2, err := forwarder.Load(with(prod, "ANVILKIT_FORWARDER_NATS_CREDS_FILE="+creds), 2)
	if err != nil {
		t.Fatal(err)
	}
	if g2.Digest != g.Digest || g2.SecretRevision == g.SecretRevision {
		t.Fatal("a credential rotation must change the secret revision and nothing else")
	}

	for name, c := range map[string]struct {
		env  []string
		want string
	}{
		"credential required outside development": {prod, "ANVILKIT_FORWARDER_NATS_CREDS_FILE is required outside development"},
		"garbage":                       {with(prod, "ANVILKIT_FORWARDER_NATS_CREDS_FILE="+garbage), "ANVILKIT_FORWARDER_NATS_CREDS_FILE: neither a NATS user credentials file (.creds) nor an NKey user seed"},
		"missing file":                  {with(prod, "ANVILKIT_FORWARDER_NATS_CREDS_FILE="+filepath.Join(dir, "absent")), "ANVILKIT_FORWARDER_NATS_CREDS_FILE: open"},
		"plaintext outside development": {with(prod, "ANVILKIT_FORWARDER_NATS_CREDS_FILE="+creds, "ANVILKIT_FORWARDER_NATS_TLS_MODE=development"), "DEVELOPMENT_ENABLED=true"},
	} {
		_, err := forwarder.Load(c.env, 1)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", name, err, c.want)
		} else if strings.Contains(err.Error(), "SECRETVALUE") {
			t.Errorf("%s: the credential content was echoed: %v", name, err)
		}
	}

	// Under the guard no credential is admitted, a placed one is still
	// classified and used, a broken one is still rejected.
	dev := []string{"ANVILKIT_FORWARDER_DATABASE_URL=postgres://u:p@127.0.0.1:5432/anvilkit_knowledge?sslmode=disable", "ANVILKIT_FORWARDER_NATS_URL=nats://127.0.0.1:4222", "ANVILKIT_FORWARDER_DEVELOPMENT_ENABLED=true", "ANVILKIT_FORWARDER_NATS_TLS_MODE=development"}
	if g, err := forwarder.Load(dev, 1); err != nil || g.Config.NATSCredsFile != "" {
		t.Fatalf("development without a credential: %v", err)
	}
	if g, err := forwarder.Load(with(dev, "ANVILKIT_FORWARDER_NATS_CREDS_FILE="+seed), 1); err != nil || !g.Config.NATSCredsNKey {
		t.Fatalf("development with a credential: %v", err)
	}
	if _, err := forwarder.Load(with(dev, "ANVILKIT_FORWARDER_NATS_CREDS_FILE="+garbage), 1); err == nil {
		t.Fatal("a broken credential is rejected under the guard too")
	}
}

// TestDatabaseTLSPolicy (P0.6): outside development the database URL, from
// the environment or the mounted file, must verify the server
// (sslmode=verify-full); the error names the mode and the variable, never
// the URL. Development keeps the foundation's sslmode=disable.
func TestDatabaseTLSPolicy(t *testing.T) {
	dir := t.TempDir()
	creds, _ := natsCredentials(t, dir)
	env := func(extra ...string) []string {
		return append([]string{"ANVILKIT_FORWARDER_NATS_URL=tls://nats.example:4222", "ANVILKIT_FORWARDER_NATS_TLS_CA_FILE=/etc/anvilkit/nats/ca.crt", "ANVILKIT_FORWARDER_NATS_CREDS_FILE=" + creds}, extra...)
	}
	const rule = "ANVILKIT_FORWARDER_DATABASE_URL: sslmode must be verify-full outside development"
	for dsn, want := range map[string]string{
		"postgres://u:hunter2@db.example:5432/anvilkit_knowledge":                                     rule + ` (got "")`,
		"postgres://u:hunter2@db.example:5432/anvilkit_knowledge?sslmode=disable":                     rule + ` (got "disable")`,
		"postgres://u:hunter2@db.example:5432/anvilkit_knowledge?sslmode=require":                     rule + ` (got "require")`,
		"postgresql://u:hunter2@db.example:5432/anvilkit_knowledge?sslmode=verify-ca":                 rule + ` (got "verify-ca")`,
		"host=db.example user=u password=hunter2 dbname=anvilkit_knowledge sslmode=verify-full":       "ANVILKIT_FORWARDER_DATABASE_URL must be a postgres URL",
		"postgres://u:hunter2@db.example:5432/anvilkit_knowledge?sslmode=verify-full&sslmode=disable": rule + ` (got "disable")`,
		"postgres://u:hunter2@db.example:5432/anvilkit_knowledge?sslmode=disable&sslmode=verify-full": "",
		"postgres://u:hunter2@db.example:5432/anvilkit_knowledge?sslmode=verify-full&ssl=true":        "ANVILKIT_FORWARDER_DATABASE_URL: the ssl parameter is not accepted outside development",
	} {
		_, err := forwarder.Load(env("ANVILKIT_FORWARDER_DATABASE_URL="+dsn), 1)
		switch {
		case want == "" && err != nil:
			t.Errorf("%s: %v", dsn, err)
		case want != "" && (err == nil || !strings.Contains(err.Error(), want)):
			t.Errorf("%s: got %v, want %q", dsn, err, want)
		case err != nil && strings.Contains(err.Error(), "hunter2"):
			t.Errorf("the URL was echoed: %v", err)
		}
	}
	if _, err := forwarder.Load(env("ANVILKIT_FORWARDER_DATABASE_URL=postgresql://u:p@db.example:5432/anvilkit_knowledge?sslmode=verify-full&sslrootcert=/etc/anvilkit/postgres-ca/ca.crt"), 1); err != nil {
		t.Fatalf("verify-full outside development: %v", err)
	}
	secret := filepath.Join(dir, "database-url")
	if err := os.WriteFile(secret, []byte("postgres://u:hunter2@db.example:5432/anvilkit_knowledge?sslmode=disable\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := forwarder.Load(env("ANVILKIT_FORWARDER_DATABASE_URL_FILE="+secret), 1); err == nil || !strings.Contains(err.Error(), `ANVILKIT_FORWARDER_DATABASE_URL_FILE: sslmode must be verify-full outside development (got "disable")`) || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("the mounted URL follows the same rule: %v", err)
	}
	if _, err := forwarder.Load([]string{"ANVILKIT_FORWARDER_DATABASE_URL=postgres://u:p@127.0.0.1:5432/anvilkit_knowledge?sslmode=disable", "ANVILKIT_FORWARDER_NATS_URL=nats://127.0.0.1:4222", "ANVILKIT_FORWARDER_DEVELOPMENT_ENABLED=true", "ANVILKIT_FORWARDER_NATS_TLS_MODE=development"}, 1); err != nil {
		t.Fatalf("development keeps sslmode=disable: %v", err)
	}
}
