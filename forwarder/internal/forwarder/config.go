// Package forwarder is the Knowledge outbox forwarder sidecar (DD-09 §2,
// C07): the watermill-sql subscriber over the outbox/outbox_offsets tables
// of anvilkit_knowledge (migration 00002; the same physical schema the
// Node owner writes and the Go owners write) and the watermill forwarder
// publishing every unwrapped envelope to its destination subject on NATS
// JetStream. It runs as anvilkit_knowledge_forwarder: message read and
// offset read/write only, no domain table, no DDL, no stream creation.
package forwarder

import (
	"bytes"
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
	// DevelopmentEnabled is the top-level DEVELOPMENT_ONLY guard: the
	// plaintext NATS transport needs NATS_TLS_MODE development and this.
	DevelopmentEnabled bool
	// NATSTLSMode is development (plaintext, guarded), tls (verify the
	// server against NATSTLSCAFile) or mtls (additionally present the
	// workload certificate). NATSTLSServerName, when set, is the name the
	// server certificate is verified against (else the URL's host).
	// Server-side TLS of the JetStream is P0.6.
	NATSTLSMode       string
	NATSTLSCAFile     string
	NATSTLSCertFile   string
	NATSTLSKeyFile    string
	NATSTLSServerName string
	// NATSCredsFile is the mounted client credential (P0.6): a NATS user
	// .creds file or, with NATSCredsNKey, a bare NKey user seed, read and
	// classified at load; required outside development, still used under
	// the guard when placed.
	NATSCredsFile string
	NATSCredsNKey bool
}

var allowed = map[string]bool{
	"DATABASE_URL": true, "DATABASE_URL_FILE": true, "NATS_URL": true, "CONSUMER_GROUP": true, "HEALTH_LISTEN": true,
	"POLL_INTERVAL": true, "ACK_DEADLINE": true, "RESEND_INTERVAL": true, "BATCH_SIZE": true, "PUBLISH_TIMEOUT": true, "CLOSE_TIMEOUT": true, "RELOAD_INTERVAL": true,
	"DEVELOPMENT_ENABLED": true, "NATS_TLS_MODE": true, "NATS_TLS_CA_FILE": true, "NATS_TLS_CERT_FILE": true, "NATS_TLS_KEY_FILE": true,
	"NATS_TLS_SERVER_NAME": true, "NATS_CREDS_FILE": true,
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
		DevelopmentEnabled: get("DEVELOPMENT_ENABLED", "false") == "true",
		NATSTLSMode:        get("NATS_TLS_MODE", "tls"), NATSTLSCAFile: values["NATS_TLS_CA_FILE"], NATSTLSCertFile: values["NATS_TLS_CERT_FILE"], NATSTLSKeyFile: values["NATS_TLS_KEY_FILE"],
		NATSTLSServerName: values["NATS_TLS_SERVER_NAME"], NATSCredsFile: values["NATS_CREDS_FILE"],
	}
	switch c.NATSTLSMode {
	case "development":
		if !c.DevelopmentEnabled {
			errs = append(errs, fmt.Errorf("%sNATS_TLS_MODE development (plaintext) requires %sDEVELOPMENT_ENABLED=true (DEVELOPMENT_ONLY)", envPrefix, envPrefix))
		}
	case "tls":
		if c.NATSTLSCAFile == "" {
			errs = append(errs, fmt.Errorf("%sNATS_TLS_CA_FILE is required under %sNATS_TLS_MODE tls", envPrefix, envPrefix))
		}
	case "mtls":
		if c.NATSTLSCAFile == "" || c.NATSTLSCertFile == "" || c.NATSTLSKeyFile == "" {
			errs = append(errs, fmt.Errorf("%sNATS_TLS_CA_FILE, NATS_TLS_CERT_FILE and NATS_TLS_KEY_FILE are required under %sNATS_TLS_MODE mtls", envPrefix, envPrefix))
		}
	default:
		errs = append(errs, fmt.Errorf("%sNATS_TLS_MODE must be development, tls or mtls", envPrefix))
	}
	// Outside development the TLS rules above already exclude the plaintext
	// mode; the connection also authenticates. Under the guard a placed
	// credential is still presented, none is admitted.
	natsCreds := ""
	if c.NATSCredsFile != "" {
		kind, revision, err := readNATSCredentials(c.NATSCredsFile)
		if err != nil {
			errs = append(errs, fmt.Errorf("%sNATS_CREDS_FILE: %w", envPrefix, err))
		}
		c.NATSCredsNKey, natsCreds = kind == natsCredsNKey, revision
	} else if !c.DevelopmentEnabled {
		errs = append(errs, fmt.Errorf("%sNATS_CREDS_FILE is required outside development (a NATS user .creds file or an NKey user seed)", envPrefix))
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
	// Neither the URL nor its parse error is ever echoed: it carries the
	// credential.
	if c.DatabaseURL == "" {
		errs = append(errs, fmt.Errorf("%sDATABASE_URL or %sDATABASE_URL_FILE is required", envPrefix, envPrefix))
	} else if u, err := url.Parse(c.DatabaseURL); err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		errs = append(errs, fmt.Errorf("%sDATABASE_URL must be a postgres URL", envPrefix))
	} else if !c.DevelopmentEnabled {
		// Outside development the connection verifies the server's chain and
		// host name (P0.6); development keeps sslmode=disable.
		source := "DATABASE_URL"
		if values["DATABASE_URL"] == "" {
			source = "DATABASE_URL_FILE"
		}
		q := u.Query()
		// The last occurrence wins, as in the driver (pgx follows libpq).
		mode := ""
		if v := q["sslmode"]; len(v) > 0 {
			mode = v[len(v)-1]
		}
		if mode != "verify-full" {
			errs = append(errs, fmt.Errorf("%s%s: sslmode must be verify-full outside development (got %q)", envPrefix, source, mode))
		}
		// pgx reads ssl=true in a URL as sslmode=require.
		if q.Has("ssl") {
			errs = append(errs, fmt.Errorf("%s%s: the ssl parameter is not accepted outside development (use sslmode=verify-full)", envPrefix, source))
		}
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
	nonSecret := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%d|%s|%s|%s|%t|%s|%s|%s|%s|%s|%s", c.NATSURL, c.ConsumerGroup, c.HealthListen, c.PollInterval, c.AckDeadline, c.ResendInterval, c.BatchSize, c.PublishTimeout, c.CloseTimeout, c.ReloadInterval, c.DevelopmentEnabled, c.NATSTLSMode, c.NATSTLSCAFile, c.NATSTLSCertFile, c.NATSTLSKeyFile, c.NATSTLSServerName, c.NATSCredsFile)
	// The secret revision covers the credential's digest as well, so that a
	// rotated credential file produces a new generation with a fresh
	// connection.
	secret := "database.url=" + c.DatabaseURL
	if natsCreds != "" {
		secret += "\nnats.creds_file=" + natsCreds
	}
	return Generation{Number: number, Config: c, Digest: digestOf(nonSecret), SecretRevision: digestOf(secret)}, nil
}

func digestOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// The kinds of a NATS client credential file.
const (
	natsCredsUser = "user"
	natsCredsNKey = "nkey"
)

// natsUserJWTMarker opens the user JWT block of a NATS .creds file.
var natsUserJWTMarker = []byte("-----BEGIN NATS USER JWT-----")

// readNATSCredentials reads and classifies a mounted NATS client
// credential: a user .creds file (it holds the decorated user JWT) or a
// bare NKey user seed (the first non-empty line starts with SU). It returns
// the kind and the content's digest; the content itself never leaves this
// function, not even in an error, and the buffer is wiped. The connection
// does not use what is read here: its options read the file themselves
// when they authenticate (natsCredentials), and the digest makes a
// rotation a new generation.
func readNATSCredentials(path string) (kind, digest string, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	defer clear(raw)
	switch {
	case bytes.Contains(raw, natsUserJWTMarker):
		kind = natsCredsUser
	case bytes.HasPrefix(firstNonEmptyLine(raw), []byte("SU")):
		kind = natsCredsNKey
	default:
		return "", "", errors.New("neither a NATS user credentials file (.creds) nor an NKey user seed")
	}
	sum := sha256.Sum256(raw)
	return kind, "sha256:" + hex.EncodeToString(sum[:]), nil
}

func firstNonEmptyLine(raw []byte) []byte {
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if line = bytes.TrimSpace(line); len(line) > 0 {
			return line
		}
	}
	return nil
}
