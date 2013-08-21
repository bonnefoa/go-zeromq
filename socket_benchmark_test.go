package zmq

import (
	"testing"
    "sync"
)

func benchamrkSimplePart(b *testing.B, sizeData int, endpoint string) {
	env := &Env{Tester: b, serverType: Pull, endpoint: endpoint, clientType: Push}
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

func benchmarkMultipartPullPush(b *testing.B, numParts int, sizeData int, endpoint string) {
    env := &Env{Tester: b, serverType: Pull, endpoint: endpoint, clientType: Push}
	env.setupServer()
	defer env.destroyServer()
    wg := &sync.WaitGroup{}
    wg.Add(1)

    go func() {
        data := makeMultipartData(numParts, sizeData)
        env.setupClient()
        defer env.destroyClient()
        for i := 0; i < b.N; i++ {
            err := env.client.SendMultipart(data, 0)
            if err != nil {
                env.Fatalf("Err on receive %q", env)
            }
        }
        wg.Wait()
    }()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
        rep, err := env.server.RecvMultipart(0)
        if err != nil {
            env.Fatalf("Err on receive %q", env)
        }
        rep.Close()
	}
    wg.Done()
	b.StopTimer()
}

func Benchmark1x10BMultipartTcp(b *testing.B) {
	benchmarkMultipartPullPush(b, 1, 10, TcpEndpoint)
}

func Benchmark10BMultipartTcp(b *testing.B) {
	benchmarkMultipartPullPush(b, 10, 1, TcpEndpoint)
}

func Benchmark10KBMultipartTcp(b *testing.B) {
	benchmarkMultipartPullPush(b, 10, 1e3, TcpEndpoint)
}

func Benchmark10MBMultipartTcp(b *testing.B) {
	benchmarkMultipartPullPush(b, 10, 1e6, TcpEndpoint)
}

func Benchmark10BMultipartInproc(b *testing.B) {
	benchmarkMultipartPullPush(b, 10, 1, InprocEndpoint+"_1b")
}

func Benchmark10KBMultipartInproc(b *testing.B) {
	benchmarkMultipartPullPush(b, 10, 1e3, InprocEndpoint+"_1K")
}

func Benchmark10MBMultipartInproc(b *testing.B) {
	benchmarkMultipartPullPush(b, 10, 1e6, InprocEndpoint+"_1M")
}

func BenchmarkMultipartRouter(b *testing.B) {
	env := &Env{Tester: b, serverType: Router, endpoint: TcpEndpoint, clientType: Req}

    env.setupServer()
    defer env.destroyServer()

    wg := &sync.WaitGroup{}
    wg.Add(1)

    go func() {
        data := makeMultipartData(2, 10)
        env.setupClient()
        defer env.destroyClient()
        for i := 0; i < b.N; i++ {
            env.client.SendMultipart(data, 0)
            rep, err := env.client.RecvMultipart(0)
            if err != nil{
                env.Fatal(err)
            }
            rep.Close()
        }
        wg.Wait()
    }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
            rep, err := env.server.RecvMultipart(0)
            if err != nil{
                env.Fatal(err)
            }
            err = env.server.SendMultipart(rep.Data, 0)
            if err != nil{
                env.Fatal(err)
            }
	}
    wg.Done()
	b.StopTimer()
}
