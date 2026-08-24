package client

import "testing"

// Unit tests for the sequence-gap RTP loss tracker used by the stress test's
// per-client downlink loss report.
//
// Run:  go test ./test/client/ -run TestRTPLossTracker -v

func TestRTPLossTrackerNoLoss(t *testing.T) {
	var tr rtpLossTracker
	for i := 0; i < 100; i++ {
		tr.note(uint16(1000 + i))
	}
	if tr.received != 100 || tr.lost != 0 {
		t.Fatalf("got received=%d lost=%d, want 100/0", tr.received, tr.lost)
	}
}

func TestRTPLossTrackerGap(t *testing.T) {
	var tr rtpLossTracker
	// seq 1000, then jump to 1005 → 4 packets lost in between
	tr.note(1000)
	tr.note(1005)
	if tr.received != 2 || tr.lost != 4 {
		t.Fatalf("got received=%d lost=%d, want 2/4", tr.received, tr.lost)
	}
	// a single packet gap
	tr.note(1007) // lost 1006
	if tr.lost != 5 {
		t.Fatalf("lost=%d, want 5", tr.lost)
	}
}

func TestRTPLossTrackerWrap(t *testing.T) {
	var tr rtpLossTracker
	tr.note(65534)
	tr.note(65535)
	// wrap to 0: nothing lost
	tr.note(0)
	// 0 → 3: lost 1,2
	tr.note(3)
	if tr.received != 4 || tr.lost != 2 {
		t.Fatalf("got received=%d lost=%d, want 4/2", tr.received, tr.lost)
	}
}

func TestRTPLossTrackerRetransmitAndDuplicate(t *testing.T) {
	var tr rtpLossTracker
	tr.note(1000)
	tr.note(1001)
	// out-of-order / NACK retransmit of an already-seen packet: backwards, no loss
	tr.note(1000)
	// duplicate delivery of the current packet: no loss
	tr.note(1001)
	if tr.received != 4 || tr.lost != 0 {
		t.Fatalf("got received=%d lost=%d, want 4/0", tr.received, tr.lost)
	}
	// an actually-new packet still tracks forward from the last high-water mark
	tr.note(1002)
	if tr.received != 5 || tr.lost != 0 {
		t.Fatalf("got received=%d lost=%d, want 5/0", tr.received, tr.lost)
	}
}

func TestRTPLossTrackerStreamRestartJump(t *testing.T) {
	var tr rtpLossTracker
	tr.note(1000)
	// re-key / renegotiation restarts sequence far ahead — must not count as loss
	tr.note(50000)
	if tr.lost != 0 {
		t.Fatalf("lost=%d, want 0 (large jump ignored)", tr.lost)
	}
	if tr.received != 2 {
		t.Fatalf("received=%d, want 2", tr.received)
	}
}

func TestRTPLossTrackerPercent(t *testing.T) {
	var tr rtpLossTracker
	// 4 received, 1 lost (1001) → 20%
	tr.note(1000)
	tr.note(1002) // lost 1001
	tr.note(1003)
	tr.note(1004)
	recv, lost, pct := tr.received, tr.lost, 0.0
	if recv+lost > 0 {
		pct = float64(lost) * 100 / float64(recv+lost)
	}
	if recv != 4 || lost != 1 || pct != 20 {
		t.Fatalf("got received=%d lost=%d pct=%v, want 4/1/20", recv, lost, pct)
	}
}
