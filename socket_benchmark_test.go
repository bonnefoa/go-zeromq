package zmq

import (
	"testing"
)

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
	benchamrkSimplePart(b, 1, TcpEndpoint)
}

func Benchmark1KBSendReceiveTcp(b *testing.B) {
	benchamrkSimplePart(b, 1e3, TcpEndpoint)
}

func Benchmark1MBSendReceiveTcp(b *testing.B) {
	benchamrkSimplePart(b, 1e6, TcpEndpoint)
}

func Benchmark1BSendReceiveInproc(b *testing.B) {
	benchamrkSimplePart(b, 1, InprocEndpoint+"_1b")
}

func Benchmark1KBSendReceiveInproc(b *testing.B) {
	benchamrkSimplePart(b, 1e3, InprocEndpoint+"_1K")
}

func Benchmark1MBSendReceiveInproc(b *testing.B) {
	benchamrkSimplePart(b, 1e6, InprocEndpoint+"_1M")
}

func makeMultipartData(numParts int, sizeData int) [][]byte {
	data := make([][]byte, numParts)
	for i := 0; i < numParts; i++ {
		data[i] = make([]byte, sizeData)
	}
	return data
}

func benchmarkMultipart(b *testing.B, numParts int, sizeData int, endpoint string) {
	env := &Env{Tester: b, serverType: PULL, endpoint: endpoint, clientType: PUSH}
	env.setupEnv()
	defer env.destroyEnv()

	data := makeMultipartData(numParts, sizeData)

	b.ResetTimer()
	var rep *MessageMultipart
	for i := 0; i < b.N; i++ {
		env.client.SendMultipart(data, 0)
		rep, _ = env.server.RecvMultipart(0)
		rep.Close()
	}
	b.StopTimer()
}

func Benchmark10BMultipartTcp(b *testing.B) {
	benchmarkMultipart(b, 10, 1, TcpEndpoint)
}

func Benchmark10KBMultipartTcp(b *testing.B) {
	benchmarkMultipart(b, 10, 1e3, TcpEndpoint)
}

func Benchmark10MBMultipartTcp(b *testing.B) {
	benchmarkMultipart(b, 10, 1e6, TcpEndpoint)
}

func Benchmark10BMultipartInproc(b *testing.B) {
	benchmarkMultipart(b, 10, 1, InprocEndpoint+"_1b")
}

func Benchmark10KBMultipartInproc(b *testing.B) {
	benchmarkMultipart(b, 10, 1e3, InprocEndpoint+"_1K")
}

func Benchmark10MBMultipartInproc(b *testing.B) {
	benchmarkMultipart(b, 10, 1e6, InprocEndpoint+"_1M")
}
