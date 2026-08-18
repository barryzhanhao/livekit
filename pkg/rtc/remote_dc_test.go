package rtc

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/rtc/transport"
	"github.com/livekit/protocol/livekit"
)

// TestRemotePCDataChannelBridgingProtocol verifies the room-side data-channel
// bridging protocol over the control channel: CreateDataChannel and
// SendDataMessage emit the right requests, and an inbound event_data_message from
// the edge is dispatched to the OnDataMessage callback. (The edge executor's real
// DC wiring + the full client loop are covered by the E2E `data` scenario.)
func TestRemotePCDataChannelBridgingProtocol(t *testing.T) {
	a, b := transport.NewLocalControlChannelPair(16)
	defer b.Close()

	remote := NewRemotePeerConnection(b)
	defer remote.Close()

	// a mock edge: read requests, respond to create/send, and push a data event.
	got := make(chan string, 8)
	go func() {
		for {
			raw, err := a.Receive()
			if err != nil {
				return
			}
			var msg remotePCMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				continue
			}
			switch msg.Op {
			case remotePCOpCreateDataChannel:
				var req remotePCCreateDataChannelRequest
				_ = json.Unmarshal(msg.Body, &req)
				got <- "create:" + req.Label
				resp, _ := json.Marshal(remotePCMessage{ID: msg.ID, Kind: remotePCKindResponse, Body: json.RawMessage("0")})
				_ = a.Send(resp)
			case remotePCOpSendDataMessage:
				var req remotePCSendDataMessageRequest
				_ = json.Unmarshal(msg.Body, &req)
				got <- "send:" + string(req.Data)
				resp, _ := json.Marshal(remotePCMessage{ID: msg.ID, Kind: remotePCKindResponse})
				_ = a.Send(resp)
			}
		}
	}()

	// create a reliable data channel (facade on the room side)
	_, err := remote.CreateDataChannel(ReliableDataChannel, nil)
	require.NoError(t, err)
	require.Equal(t, "create:"+ReliableDataChannel, <-got)

	// register the inbound callback, then simulate the edge forwarding a client
	// data message over the control channel.
	gotMsg := make(chan struct {
		kind livekit.DataPacket_Kind
		data string
	}, 1)
	remote.OnDataMessage(func(kind livekit.DataPacket_Kind, data []byte) {
		gotMsg <- struct {
			kind livekit.DataPacket_Kind
			data string
		}{kind, string(data)}
	})
	event, _ := json.Marshal(remotePCMessage{
		Kind: remotePCKindEvent,
		Op:   remotePCOpEventDataMessage,
		Body: mustJSON(remotePCDataMessageEvent{Kind: int32(livekit.DataPacket_RELIABLE), Data: []byte("hello")}),
	})
	require.NoError(t, a.Send(event))
	select {
	case m := <-gotMsg:
		require.Equal(t, livekit.DataPacket_RELIABLE, m.kind)
		require.Equal(t, "hello", m.data)
	case <-time.After(2 * time.Second):
		t.Fatal("OnDataMessage callback not invoked")
	}

	// outbound: SendDataMessage emits a send_data_message request with the payload
	require.NoError(t, remote.SendDataMessage(livekit.DataPacket_LOSSY, []byte("ping")))
	require.Equal(t, "send:ping", <-got)
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
