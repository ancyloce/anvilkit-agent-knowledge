package forwarder

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// TestNATSOptions (P0.6): the verified transport names the configured
// server; a bare seed authenticates as its NKey user, a .creds file as its
// JWT user, and both read the mounted file when the connection
// authenticates (a rotated .creds file is presented on the next
// reconnect); the plaintext development connection presents nothing it was
// not given; a broken seed is refused without echoing it.
func TestNATSOptions(t *testing.T) {
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	seed, _ := kp.Seed()
	pub, _ := kp.PublicKey()
	seedFile := filepath.Join(dir, "user.nk")
	if err := os.WriteFile(seedFile, append(seed, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	credsFile := filepath.Join(dir, "user.creds")
	writeCreds := func(jwt string) {
		content := "-----BEGIN NATS USER JWT-----\n" + jwt + "\n------END NATS USER JWT------\n\n-----BEGIN USER NKEY SEED-----\n" + string(seed) + "\n------END USER NKEY SEED------\n"
		if err := os.WriteFile(credsFile, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeCreds("eyJhbGciOiJlZDI1NTE5LW5rZXkifQ.b25l.c2ln")
	apply := func(c Config) nats.Options {
		t.Helper()
		opts, err := natsOptions(c)
		if err != nil {
			t.Fatal(err)
		}
		var o nats.Options
		for _, opt := range opts {
			if err := opt(&o); err != nil {
				t.Fatal(err)
			}
		}
		return o
	}
	nonce := []byte("server-nonce")

	o := apply(Config{NATSTLSMode: "tls", NATSTLSCAFile: ca, NATSTLSServerName: "nats.anvilkit-system.svc", NATSCredsFile: seedFile, NATSCredsNKey: true})
	if !o.Secure || o.TLSConfig == nil || o.TLSConfig.ServerName != "nats.anvilkit-system.svc" {
		t.Fatalf("the server is verified by the configured name: %+v", o.TLSConfig)
	}
	if o.Nkey != pub || o.UserJWT != nil {
		t.Fatalf("a bare seed authenticates as its NKey user: %q", o.Nkey)
	}
	if sig, err := o.SignatureCB(nonce); err != nil || kp.Verify(nonce, sig) != nil {
		t.Fatalf("nonce signature: %v", err)
	}
	if o = apply(Config{NATSTLSMode: "tls", NATSTLSCAFile: ca}); o.TLSConfig.ServerName != "" {
		t.Fatalf("without a configured name the URL's host is verified: %q", o.TLSConfig.ServerName)
	}

	o = apply(Config{NATSTLSMode: "tls", NATSTLSCAFile: ca, NATSCredsFile: credsFile})
	if jwt, err := o.UserJWT(); err != nil || jwt != "eyJhbGciOiJlZDI1NTE5LW5rZXkifQ.b25l.c2ln" {
		t.Fatalf("user JWT: %q %v", jwt, err)
	}
	if sig, err := o.SignatureCB(nonce); err != nil || kp.Verify(nonce, sig) != nil {
		t.Fatalf("nonce signature: %v", err)
	}
	writeCreds("eyJhbGciOiJlZDI1NTE5LW5rZXkifQ.dHdv.c2ln")
	if jwt, err := o.UserJWT(); err != nil || jwt != "eyJhbGciOiJlZDI1NTE5LW5rZXkifQ.dHdv.c2ln" {
		t.Fatalf("the rotated file is read at the next authentication: %q %v", jwt, err)
	}

	if o = apply(Config{NATSTLSMode: "development"}); o.Secure || o.TLSConfig != nil || o.Nkey != "" || o.UserJWT != nil {
		t.Fatal("the development connection is plaintext and unauthenticated without a credential")
	}

	broken := filepath.Join(dir, "broken.nk")
	if err := os.WriteFile(broken, []byte("SUNOTASEEDSECRETVALUE\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := natsOptions(Config{NATSTLSMode: "development", NATSCredsFile: broken, NATSCredsNKey: true}); err == nil || strings.Contains(err.Error(), "SECRETVALUE") {
		t.Fatalf("a broken seed is refused without echoing it: %v", err)
	}
}
