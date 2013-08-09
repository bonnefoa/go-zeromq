package zmq

import (
	"reflect"
	"testing"
	"time"
)

const TCP_ENDPOINT = "tcp://127.0.0.1:9999"
const INPROC_ENDPOINT = "inproc://test_proc"

type Tester interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

type Env struct {
	*Context
	server     *Socket
	client     *Socket
	endpoint   string
	serverType SocketType
	clientType SocketType
	Tester
}

func (env *Env) setupSocket(tp SocketType, soc **Socket, isServer bool) {
	var err error
	if tp == 0 {
		return
	}
	*soc, err = env.NewSocket(tp)
	if err != nil {
		env.Fatal("Error on input socket creation", err)
	}
	if env.endpoint == "" {
		return
	}
	if isServer {
		err = (*soc).Bind(env.endpoint)
	} else {
		err = (*soc).Connect(env.endpoint)
	}
	if err != nil {
		env.Fatal("Error on socket bind", err)
	}
}

func (env *Env) setupEnv() {
	var err error
	env.Context, err = NewContext()
	if err != nil {
		env.Fatal("Error on context creation", err)
	}

	env.setupSocket(env.serverType, &env.server, true)
	env.setupSocket(env.clientType, &env.client, false)
}

func (env *Env) destroyEnv() {
	if env.server != nil {
		env.server.Unbind(env.endpoint)
		env.server.Close()
	}
	if env.client != nil {
		env.client.Disconnect(env.endpoint)
		env.client.Close()
	}
	// Avoid hangup by executing destroy in a goroutine
	go func() {
		err := env.Destroy()
		if err != nil {
			env.Fatal("Error on context destruction", err)
		}
	}()
}

func TestSocketTcpBind(t *testing.T) {
	env := &Env{Tester: t, serverType: ROUTER}
	env.setupEnv()
	defer env.destroyEnv()
	err := env.server.Bind(TCP_ENDPOINT)
	if err != nil {
		t.Fatal("Error on socket bind", err)
	}
	err = env.server.Unbind(TCP_ENDPOINT)
	if err != nil {
		t.Fatal("Error on socket unbind", err)
	}
}

func TestSocketSend(t *testing.T) {
	env := &Env{Tester: t, serverType: REP, endpoint: TCP_ENDPOINT, clientType: REQ}
	env.setupEnv()
	defer env.destroyEnv()
	data := []byte("test")
	// Client send request
	err := env.client.Send(data, 0)
	if err != nil {
		t.Fatal("Error on client request send", err)
	}
	// Server receive request
	response, err := env.server.Recv(0)
	defer response.Close()
	if err != nil {
		t.Fatal("Error on server request receive", err)
	}
	if !reflect.DeepEqual(response.Data, data) {
		t.Fatalf("server received response %v != sended data %v", response.Data, data)
	}
	// Server send response
	err = env.server.Send(response.Data, 0)
	if err != nil {
		t.Fatal("Error on server response sending", err)
	}
	// client receive response
	response, err = env.client.Recv(0)
	defer response.Close()
	if err != nil {
		t.Fatal("Error on client response receive", err)
	}
	if !reflect.DeepEqual(response.Data, data) {
		t.Fatalf("client received response %v != sended data %v", response.Data, data)
	}
}

func TestMultipart(t *testing.T) {
	env := &Env{Tester: t, serverType: PULL, endpoint: TCP_ENDPOINT, clientType: PUSH}
	env.setupEnv()
	defer env.destroyEnv()

	data := [][]byte{[]byte("test"), []byte("test2"), []byte("test3")}
	err := env.client.SendMultipart(data, 0)
	if err != nil {
		t.Fatal("Error on multipart send", err)
	}
	rep, err := env.server.RecvMultipart(0)
	if err != nil {
		t.Fatal("Error on multipart receive", err)
	}
	defer rep.Close()
	if !reflect.DeepEqual(rep.Data, data) {
		t.Fatalf("Multipart Receive %q, expected %q", rep.Data, data)
	}
}

func TestGetSocketOption(t *testing.T) {
	env := &Env{Tester: t, serverType: PULL, endpoint: TCP_ENDPOINT, clientType: PUSH}
	env.setupEnv()
	defer env.destroyEnv()
	rc, err := env.server.GetOptionInt(TYPE)
	if err != nil {
		t.Fatal("Error on socket type get", err)
	}
	if SocketType(rc) != PULL {
		t.Fatal("Expected type to be PULL, got ", rc)
	}
	endpoint, err := env.server.GetOptionString(LAST_ENDPOINT)
	if err != nil {
		t.Fatal("Error on socket endpoint get", err)
	}
	if endpoint != TCP_ENDPOINT {
		t.Fatalf("Expected last endpoint to be %q, got %q", TCP_ENDPOINT, endpoint)
	}
}

func TestSetSocketOption(t *testing.T) {
	env := &Env{Tester: t, serverType: PULL, endpoint: TCP_ENDPOINT, clientType: PUSH}
	env.setupEnv()
	defer env.destroyEnv()

	err := env.server.SetOptionInt(RATE, 1500)
	if err != nil {
		t.Fatal("Error on socket rate set", err)
	}
	rate, err := env.server.GetOptionInt(RATE)
	if err != nil {
		t.Fatal("Error on socket rate get", err)
	}
	if rate != 1500 {
		t.Fatal("Expected rate to be 1500, got ", rate)
	}
	err = env.server.SetOptionUint64(AFFINITY, 1)
	if err != nil {
		t.Fatal("Error on socket affinity set", err)
	}
	affinity, err := env.server.GetOptionUint64(AFFINITY)
	if err != nil {
		t.Fatal("Error on socket affinity get", err)
	}
	if affinity != 1 {
		t.Fatal("Expected affinity to be 1, got", affinity)
	}
}

func TestSocketSubscribe(t *testing.T) {
	env := &Env{Tester: t, serverType: PUB, endpoint: TCP_ENDPOINT, clientType: SUB}
	env.setupEnv()
	defer env.destroyEnv()
	totoData := []byte("toto mess")
	testData := []byte("test mess")
	testTopic := "test"
	totoTopic := "toto"
	err := env.client.SetOptionString(SUBSCRIBE, &testTopic)
	<-time.After(time.Millisecond)
	if err != nil {
		t.Fatal("Error on socket subscribe, got", err)
	}
	env.server.Send(totoData, 0)
	env.server.Send(testData, 0)
	response, err := env.client.Recv(0)
	if err != nil {
		t.Fatal("Error on receive, got", err)
	}
	if !reflect.DeepEqual(response.Data, testData) {
		t.Fatal("Expected message 'test mess', got ", response.Data)
	}
	err = env.client.SetOptionString(UNSUBSCRIBE, &testTopic)
	if err != nil {
		t.Fatal("Error on socket unsubscribe, got", err)
	}
	err = env.client.SetOptionString(SUBSCRIBE, &totoTopic)
	if err != nil {
		t.Fatal("Error on socket subscribe, got", err)
	}
	<-time.After(time.Millisecond)
	err = env.server.Send(testData, 0)
	if err != nil {
		t.Fatal("Error on send, got", err)
	}
	err = env.server.Send(totoData, 0)
	if err != nil {
		t.Fatal("Error on send, got", err)
	}
	response, err = env.client.Recv(0)
	if !reflect.DeepEqual(response.Data, totoData) {
		t.Fatal("Expected message 'toto mess', got ", string(response.Data))
	}
}

func benchamrkSimplePart(b *testing.B, sizeData int, endpoint string) {
	env := &Env{Tester: b, serverType: PULL, endpoint: endpoint, clientType: PUSH}
	env.setupEnv()
	defer env.destroyEnv()

	data := make([]byte, sizeData)

	b.ResetTimer()
	var rep *MessagePart
	for i := 0; i < b.N; i++ {
		err := env.client.Send(data, 0)
		if err != nil {
			b.Fatal(err)
		}
		rep, err = env.server.Recv(0)
		if err != nil {
			b.Fatal(err)
		}
		err = rep.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func Benchmark1BSendReceiveTcp(b *testing.B) {
	benchamrkSimplePart(b, 1, TCP_ENDPOINT)
}

func Benchmark1KBSendReceiveTcp(b *testing.B) {
	benchamrkSimplePart(b, 1e3, TCP_ENDPOINT)
}

func Benchmark1MBSendReceiveTcp(b *testing.B) {
	benchamrkSimplePart(b, 1e6, TCP_ENDPOINT)
}

func Benchmark1BSendReceiveInproc(b *testing.B) {
	benchamrkSimplePart(b, 1, INPROC_ENDPOINT+"_1b")
}

func Benchmark1KBSendReceiveInproc(b *testing.B) {
	benchamrkSimplePart(b, 1e3, INPROC_ENDPOINT+"_1K")
}

func Benchmark1MBSendReceiveInproc(b *testing.B) {
	benchamrkSimplePart(b, 1e6, INPROC_ENDPOINT+"_1M")
}

func benchmarkMultipart(b *testing.B, numParts int, sizeData int, endpoint string) {
	env := &Env{Tester: b, serverType: PULL, endpoint: endpoint, clientType: PUSH}
	env.setupEnv()
	defer env.destroyEnv()

	data := make([][]byte, numParts)
	for i := 0; i < numParts; i++ {
		data[i] = make([]byte, sizeData)
	}

	b.ResetTimer()
	var rep *MessageMultipart
	for i := 0; i < b.N; i++ {
		err := env.client.SendMultipart(data, 0)
		if err != nil {
			b.Fatal(err)
		}
		rep, err = env.server.RecvMultipart(0)
		if err != nil {
			b.Fatal(err)
		}
		err = rep.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func Benchmark10BMultipartTcp(b *testing.B) {
	benchmarkMultipart(b, 10, 1, TCP_ENDPOINT)
}

func Benchmark10KBMultipartTcp(b *testing.B) {
	benchmarkMultipart(b, 10, 1e3, TCP_ENDPOINT)
}

func Benchmark10MBMultipartTcp(b *testing.B) {
	benchmarkMultipart(b, 10, 1e6, TCP_ENDPOINT)
}

func Benchmark10BMultipartInproc(b *testing.B) {
	benchmarkMultipart(b, 10, 1, INPROC_ENDPOINT+"_1b")
}

func Benchmark10KBMultipartInproc(b *testing.B) {
	benchmarkMultipart(b, 10, 1e3, INPROC_ENDPOINT+"_1K")
}

func Benchmark10MBMultipartInproc(b *testing.B) {
	benchmarkMultipart(b, 10, 1e6, INPROC_ENDPOINT+"_1M")
}
