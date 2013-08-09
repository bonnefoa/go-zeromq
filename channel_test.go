package zmq

import (
	"testing"
	"reflect"
)

func TestChannel(t *testing.T) {
	env := &Env{Tester: t}
	env.setupEnv()
	defer env.destroyEnv()
	data := [][]byte{ []byte("test") }

	repSc, err := env.NewSocketChannel(REP, TCP_ENDPOINT, 10)
	defer repSc.Close()
	if err != nil {
		t.Fatal("Got error on rep channel creation: ", err)
	}

	reqSc, err := env.NewSocketChannel(REQ, TCP_ENDPOINT, 10)
	if err != nil {
		t.Fatal("Got error on req channel creation: ", err)
	}

	reqSc.SendChannel <- data
	defer reqSc.Close()
	parts := <-repSc.RecvChannel
	defer parts.Close()
	if !reflect.DeepEqual(data, parts.Data) {
		t.Fatalf("Expected request to be %s, got %s", data, parts.Data)
	}
}


func benchmarkChannel(b *testing.B, numParts int, sizeData int, endpoint string) {
	env := &Env{Tester: b}
	env.setupEnv()
	defer env.destroyEnv()

	data := makeMultipartData(numParts, sizeData)

	pullSc, _ := env.NewSocketChannel(PULL, TCP_ENDPOINT, 10)
	pushSc, _ := env.NewSocketChannel(PUSH, TCP_ENDPOINT, 10)

	var rep *MessageMultipart
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pushSc.SendChannel <- data
		rep = <-pullSc.RecvChannel
		rep.Close()
	}
	b.StopTimer()
	pullSc.Close()
	pushSc.Close()
}

func BenchmarkChannel10BTcp(b *testing.B) {
	benchmarkChannel(b, 10, 1, TCP_ENDPOINT)
}

func BenchmarkChannel10KBTcp(b *testing.B) {
	benchmarkChannel(b, 10, 1e3, TCP_ENDPOINT)
}

func BenchmarkChannel10MBTcp(b *testing.B) {
	benchmarkChannel(b, 10, 1e6, TCP_ENDPOINT)
}
