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
	env.setupEnv()
	defer env.destroyEnv()

	data := makeMultipartData(numParts, sizeData)
    wg := &sync.WaitGroup{}
    wg.Add(1)

    go func() {
        pollReceive := &PollItem{Socket: env.server, Events: Pollin}
        pollers := PollItems{pollReceive}
        for {
            _, err := pollers.Poll(-1)
            if err != nil {
                wg.Done()
                return
            }
            rep, err := env.server.RecvMultipart(0)
            if err != nil {
                wg.Done()
                return
            }
            rep.Close()
        }
    }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.client.SendMultipart(data, 0)
	}
    env.server.Close()
    wg.Wait()
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
	env.setupEnv()
	defer env.destroyEnv()

	data := makeMultipartData(2, 10)
    wg := &sync.WaitGroup{}
    wg.Add(1)

    go func() {
        defer wg.Done()
        pollReceive := &PollItem{Socket: env.server, Events: Pollin}
        pollers := PollItems{pollReceive}
        for {
            _, err := pollers.Poll(-1)
            if err != nil {
                return
            }
            rep, err := env.server.RecvMultipart(0)
            if err != nil {
                return
            }
            err = env.server.SendMultipart(rep.Data, DontWait)
            if err != nil {
                return
            }
            rep.Close()
        }
    }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.client.SendMultipart(data, 0)
        env.client.RecvMultipart(0)
	}
    env.server.Close()
    wg.Wait()
	b.StopTimer()
}
