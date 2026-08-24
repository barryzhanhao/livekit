package transport

// Relay benchmarks for the NAT per-track TCP media channel.
//
// TestRelayThroughput: raw delivered frame rate under N concurrent connections.
// TestRelayLatencyAtFps: per-frame write→read delivery latency at realistic 30fps
// pacing (the metric that matters for subscriber jitter — Nagle + 2-write framing
// add per-frame latency/variance that a jitter buffer turns into loss).
//
//	Run:  go test ./pkg/rtc/transport/ -run 'TestRelay(Throughput|LatencyAtFps)' -v

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const relayBenchSecret = "bench-secret"

func TestRelayThroughput(t *testing.T) {
	for _, conns := range []int{1, 50, 200} {
		t.Run(fmt.Sprintf("conns=%d", conns), func(t *testing.T) {
			const duration = 2 * time.Second
			payload := make([]byte, 1200)

			ln, err := ListenTCPMediaChannel("127.0.0.1:0", relayBenchSecret)
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()

			var delivered atomic.Int64
			var wg sync.WaitGroup
			for i := 0; i < conns; i++ {
				ch, peer := acceptDial(t, ln)
				wg.Add(1)
				go func(c MediaChannel) {
					defer wg.Done()
					end := time.Now().Add(duration)
					for time.Now().Before(end) {
						if c.WriteRTP(payload) != nil {
							return
						}
					}
				}(ch)
				go func(c MediaChannel) {
					for {
						if _, err := c.ReadRTP(); err != nil {
							return
						}
						delivered.Add(1)
					}
				}(peer)
				defer peer.Close()
				defer ch.Close()
			}
			wg.Wait()
			time.Sleep(500 * time.Millisecond)
			rate := float64(delivered.Load()) / duration.Seconds()
			t.Logf("conns=%d: %.0f frames/sec aggregate, %.1f frames/sec/conn (%.0f Mbps)",
				conns, rate, rate/float64(conns), rate*float64(len(payload))*8/1e6)
		})
	}
}

func TestRelayLatencyAtFps(t *testing.T) {
	for _, conns := range []int{1, 50, 200} {
		t.Run(fmt.Sprintf("conns=%d", conns), func(t *testing.T) {
			const frames = 300
			const interval = 33 * time.Millisecond // ~30 fps
			payload := make([]byte, 1200)

			ln, err := ListenTCPMediaChannel("127.0.0.1:0", relayBenchSecret)
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()

			var mu sync.Mutex
			var allLat []time.Duration
			var wg sync.WaitGroup
			for i := 0; i < conns; i++ {
				ch, peer := acceptDial(t, ln)
				writeTimes := make([]time.Time, frames)
				wg.Add(1)
				go func(c MediaChannel) {
					defer wg.Done()
					lats := make([]time.Duration, 0, frames)
					for i := 0; i < frames; i++ {
						if _, err := c.ReadRTP(); err != nil {
							return
						}
						lats = append(lats, time.Since(writeTimes[i]))
					}
					mu.Lock()
					allLat = append(allLat, lats...)
					mu.Unlock()
				}(peer)
				go func(c MediaChannel) {
					for i := 0; i < frames; i++ {
						writeTimes[i] = time.Now()
						if c.WriteRTP(payload) != nil {
							return
						}
						time.Sleep(interval)
					}
				}(ch)
				defer peer.Close()
				defer ch.Close()
			}
			wg.Wait()

			mu.Lock()
			sort.Slice(allLat, func(a, b int) bool { return allLat[a] < allLat[b] })
			n := len(allLat)
			if n == 0 {
				t.Fatal("no frames delivered")
			}
			p50 := allLat[n/2]
			p90 := allLat[n*9/10]
			p99 := allLat[n*99/100]
			maxLat := allLat[n-1]
			mu.Unlock()
			t.Logf("conns=%d: delivery latency p50=%s p90=%s p99=%s max=%s",
				conns, p50.Round(time.Microsecond), p90.Round(time.Microsecond),
				p99.Round(time.Microsecond), maxLat.Round(time.Microsecond))
		})
	}
}

func acceptDial(t *testing.T, ln *TCPMediaChannelListener) (local, peer MediaChannel) {
	t.Helper()
	acceptCh := make(chan MediaChannel, 1)
	acceptErr := make(chan error, 1)
	go func() {
		ch, err := ln.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		acceptCh <- ch
	}()
	p, err := DialTCPMediaChannel(ln.Addr().String(), relayBenchSecret)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case c := <-acceptCh:
		return c, p
	case e := <-acceptErr:
		t.Fatal(e)
		return nil, nil
	}
}
