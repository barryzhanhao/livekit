// webhook-receiver — a minimal, production-grade LiveKit webhook receiver used by
// the NAT E2E suite to prove the server-side webhook (HTTP callback) path
// cross-node. It verifies every incoming POST with the same signature scheme the
// real webhook.verifier uses (Authorization Bearer JWT carrying the sha256 of the
// body), then logs the event. The 06-client-e2e.sh harness asserts on
// `kubectl logs deploy/webhook-receiver` for the expected event types.
//
// Deployed as its own tiny pod + ClusterIP service so both server nodes can reach
// it via the cluster DNS (webhook-receiver.default.svc:8080/webhook) — the server
// pods cannot reach the host (different subnet).
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/livekit/protocol/webhook"
)

// staticKeyProvider serves the single devkey/secret pair the server signs with.
type staticKeyProvider struct {
	key    string
	secret string
}

func (p *staticKeyProvider) GetSecret(key string) string {
	if key == p.key {
		return p.secret
	}
	return ""
}

func (p *staticKeyProvider) NumKeys() int { return 1 }

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	addr := env("WEBHOOK_ADDR", ":8080")
	kp := &staticKeyProvider{
		key:    env("WEBHOOK_API_KEY", "devkey"),
		secret: env("WEBHOOK_API_SECRET", "secret"),
	}

	http.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		event, err := webhook.ReceiveWebhookEvent(r, kp)
		if err != nil {
			log.Printf("WEBHOOK_REJECTED: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		room := event.Room
		identity := ""
		if event.Participant != nil {
			identity = event.Participant.Identity
		}
		trackID := ""
		if event.Track != nil {
			trackID = event.Track.Sid
		}
		log.Printf("WEBHOOK_EVENT %s room=%s participant=%s track=%s", event.Event, room, identity, trackID)
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "OK")
	})

	log.Printf("webhook receiver listening on %s (key=%s)", addr, kp.key)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("webhook receiver failed: %v", err)
	}
}
