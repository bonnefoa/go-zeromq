package zmq

import (
	"testing"
	"time"
)

func TestPollSocket(t *testing.T) {
	env := &Env{Tester: t, serverType: Pull, endpoint: TcpEndpoint, clientType: Push}
	env.setupEnv()
	defer env.destroyEnv()

	data := []byte("test")
	err := env.client.Send(data, 0)
	if err != nil {
		t.Fatal("Error on send", err)
	}

	item := &PollItem{Socket:env.server, Events:Pollin}
	items := PollItems{ item  }
	rc, err := items.Poll(-1 * time.Millisecond)
	if rc != 1 {
		t.Fatalf("Expected pollin to return 1, was %d, err is %q", rc, err)
	}
	if item.REvents != Pollin {
		t.Fatalf("Expected poll item to be filled with Pollin, got %+v", item)
	}
}

func BenchmarkPollSocket(b *testing.B) {
	env := &Env{Tester: b, serverType: Pull, endpoint: TcpEndpoint, clientType: Push}
	env.setupEnv()
	defer env.destroyEnv()

	data := make([]byte, 1e3)
	item := &PollItem{Socket:env.server, Events:Pollin}
	items := PollItems{ item  }
	var resp *MessagePart
	for i :=0; i < b.N; i++ {
		env.client.Send(data, 0)
		items.Poll(-1 * time.Millisecond)
		resp, _ = env.server.Recv(0)
		_ = resp.Close()
	}
}
