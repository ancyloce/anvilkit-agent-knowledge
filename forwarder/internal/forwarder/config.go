// Package forwarder is the Knowledge outbox forwarder sidecar (DD-09 §2,
// C07): the watermill-sql subscriber over the outbox/outbox_offsets tables
// of anvilkit_knowledge (migration 00002; the same physical schema the
// Node owner writes and the Go owners write) and the watermill forwarder
// publishing every unwrapped envelope to its destination subject on NATS
// JetStream. It runs as anvilkit_knowledge_forwarder: message read and
// offset read/write only, no domain table, no DDL, no stream creation.
package forwarder

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const envPrefix = "ANVILKIT_FORWARDER_"

// Config is the sidecar's complete configuration, taken from the
// allowlisted environment only (no file: the sidecar has no reviewed
// business parameters; every value is a placement, a bound or the secret).
type Config struct {
	DatabaseURL     string
	DatabaseURLFile string
	NATSURL         string
	ConsumerGroup   string
	HealthListen    string
	PollInterval    time.Duration
	AckDeadline     time.Duration
	ResendInterval  time.Duration
	BatchSize       int
	PublishTimeout  time.Duration
	CloseTimeout    time.Duration
	ReloadInterval  time.Duration
}

var allowed = map[string]bool{
	"DATABASE_URL": true, "DATABASE_URL_FILE": true, "NATS_URL": true, "CONSUMER_GROUP": true, "HEALTH_LISTEN": true,
	"POLL_INTERVAL": true, "ACK_DEADLINE": true, "RESEND_INTERVAL": true, "BATCH_SIZE": true, "PUBLISH_TIMEOUT": true, "CLOSE_TIMEOUT": true, "RELOAD_INTERVAL": true,
}

// Generation is one validated configuration with the digests of its inputs.
type Generation struct {
	Number         uint64
	Config         Config
	Digest         string
	SecretRevision string
}

// Inputs identify what a change of produces a new generation.
func (g Generation) Inputs() string { return g.Digest + "/" + g.SecretRevision }

// Load builds a generation from the environment.
func Load(environ []string, number uint64) (Generation, error) {
	values := map[string]string{}
	var unknown []string
	for _, kv := range environ {
		name, value, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, envPrefix) {
			continue
		}
		key := strings.TrimPrefix(name, envPrefix)
		if !allowed[key] {
			unknown = append(unknown, name)
			continue
		}
		values[key] = value
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return Generation{}, fmt.Errorf("config: environment variables are not allowed: %s", strings.Join(unknown, ", "))
	}
	get := func(key, def string) string {
		if v, ok := values[key]; ok {
			return v
		}
		return def
	}
	dur := func(key, def string, errs *[]error) time.Duration {
		d, err := time.ParseDuration(get(key, def))
		if err != nil {
			*errs = append(*errs, fmt.Errorf("%s%s: %w", envPrefix, key, err))
		}
		return d
	}
	var errs []error
	c := Config{
		DatabaseURL: values["DATABASE_URL"], DatabaseURLFile: values["DATABASE_URL_FILE"], NATSURL: values["NATS_URL"],
		ConsumerGroup: get("CONSUMER_GROUP", "anvilkit-agent-knowledge-forwarder"), HealthListen: get("HEALTH_LISTEN", "127.0.0.1:9125"),
		PollInterval: dur("POLL_INTERVAL", "500ms", &errs), AckDeadline: dur("ACK_DEADLINE", "30s", &errs), ResendInterval: dur("RESEND_INTERVAL", "1s", &errs),
		PublishTimeout: dur("PUBLISH_TIMEOUT", "5s", &errs), CloseTimeout: dur("CLOSE_TIMEOUT", "30s", &errs), ReloadInterval: dur("RELOAD_INTERVAL", "2s", &errs),
	}
	if n, err := strconv.Atoi(get("BATCH_SIZE", "100")); err != nil || n < 1 || n > 1000 {
		errs = append(errs, fmt.Errorf("%sBATCH_SIZE must be an integer within [1, 1000]", envPrefix))
	} else {
		c.BatchSize = n
	}
	if c.DatabaseURL == "" && c.DatabaseURLFile != "" {
		raw, err := os.ReadFile(c.DatabaseURLFile)
		if err != nil {
			errs = append(errs, fmt.Errorf("%sDATABASE_URL_FILE: %w", envPrefix, err))
		}
		c.DatabaseURL = strings.TrimSpace(string(raw))
	}
	if c.DatabaseURL == "" {
		errs = append(errs, fmt.Errorf("%sDATABASE_URL or %sDATABASE_URL_FILE is required", envPrefix, envPrefix))
	} else if u, err := url.Parse(c.DatabaseURL); err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		errs = append(errs, fmt.Errorf("%sDATABASE_URL must be a postgres URL", envPrefix))
	}
	if u, err := url.Parse(c.NATSURL); c.NATSURL == "" || err != nil || (u.Scheme != "nats" && u.Scheme != "tls") {
		errs = append(errs, fmt.Errorf("%sNATS_URL must be a nats:// or tls:// URL", envPrefix))
	}
	for name, d := range map[string]time.Duration{"POLL_INTERVAL": c.PollInterval, "ACK_DEADLINE": c.AckDeadline, "RESEND_INTERVAL": c.ResendInterval, "PUBLISH_TIMEOUT": c.PublishTimeout, "CLOSE_TIMEOUT": c.CloseTimeout, "RELOAD_INTERVAL": c.ReloadInterval} {
		if d <= 0 || d > 10*time.Minute {
			errs = append(errs, fmt.Errorf("%s%s must be within (0, 10m]", envPrefix, name))
		}
	}
	if len(errs) > 0 {
		return Generation{}, fmt.Errorf("config: %w", errors.Join(errs...))
	}
	nonSecret := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%d|%s|%s|%s", c.NATSURL, c.ConsumerGroup, c.HealthListen, c.PollInterval, c.AckDeadline, c.ResendInterval, c.BatchSize, c.PublishTimeout, c.CloseTimeout, c.ReloadInterval)
	return Generation{Number: number, Config: c, Digest: digestOf(nonSecret), SecretRevision: digestOf("database.url=" + c.DatabaseURL)}, nil
}

func digestOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:])
}
