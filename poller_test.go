package zmq

import (
	"testing"
	"time"
)

func TestPollSocket(t *testing.T) {
	env := &Env{Tester: t, serverType: PULL, endpoint: TCP_ENDPOINT, clientType: PUSH}
	env.setupEnv()
	defer env.destroyEnv()

	data := []byte("test")
	err := env.client.Send(data, 0)
	if err != nil {
		t.Fatal("Error on send", err)
	}

	item := PollItem{env.server, []PollEvent{POLLIN}}
	items := &PollItems{ item  }
	rc, err := items.Poll(-1 * time.Millisecond)
	if rc != 1 {
		t.Fatalf("Expected pollin to return 1, was %d, err is %q", rc, err)
	}
}
