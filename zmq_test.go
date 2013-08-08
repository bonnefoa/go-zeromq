package zmq

import (
	check "launchpad.net/gocheck"
	"testing"
)

const TCP_ENDPOINT = "tcp://127.0.0.1:9999"
const INPROC_ENDPOINT = "inproc://test_proc"

func TestCheck(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&ContextSuite{})
var _ = check.Suite(&SocketSuite{})

type ContextSuite struct{}

func (s *ContextSuite) TestContextCreateAndDestroy(c *check.C) {
	ctx, err := NewContext()
	c.Assert(err, check.IsNil, check.Commentf("Expected non nil context"))
	err = ctx.Destroy()
	c.Assert(err, check.IsNil, check.Commentf("Error in context destruction"))
}

type SocketSuite struct {
	*Context
}

func (s *SocketSuite) SetUpSuite(c *check.C) {
	ctx, err := NewContext()
	c.Assert(err, check.IsNil, check.Commentf("Error on context creation, expected nil error"))
	s.Context = ctx
}

func (s *SocketSuite) TearDownSuite(c *check.C) {
	go func() {
		err := s.Destroy()
		c.Assert(err, check.IsNil, check.Commentf("Error on context destruction"))
	}()
}

func (ctx *SocketSuite) TestSocketCreate(c *check.C) {
	socket, err := ctx.NewSocket(REQ)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket creation"))
	err = socket.Close()
	c.Assert(err, check.IsNil, check.Commentf("Error on socket closing"))
}

func (ctx *SocketSuite) TestSocketTcpBind(c *check.C) {
	socket, err := ctx.NewSocket(ROUTER)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket creation"))
	defer socket.Close()

	err = socket.Bind(TCP_ENDPOINT)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket bind"))
	err = socket.Unbind(TCP_ENDPOINT)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket unbind"))
}

func (ctx *SocketSuite) TestSocketSend(c *check.C) {
	data := []byte("test")

	rep_socket, err := ctx.NewSocket(REP)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket creation"))
	defer rep_socket.Close()

	req_socket, err := ctx.NewSocket(REQ)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket creation"))
	defer req_socket.Close()

	err = rep_socket.Bind(TCP_ENDPOINT)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket bind"))

	err = req_socket.Connect(TCP_ENDPOINT)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket bind"))

	err = req_socket.Send(data, 0)
	c.Assert(err, check.IsNil, check.Commentf("Error on send"))
	response, err := rep_socket.Recv(0)
	response.FreeMsg()

	c.Assert(err, check.IsNil, check.Commentf("Error on receive"))
	c.Assert(response.Data, check.DeepEquals, data)
	err = rep_socket.Send(response.Data, 0)
	c.Assert(err, check.IsNil, check.Commentf("Error on response send"))
	response, err = req_socket.Recv(0)
	c.Assert(err, check.IsNil, check.Commentf("Error on response receive"))
	c.Assert(response.Data, check.DeepEquals, data)

	err = req_socket.Disconnect(TCP_ENDPOINT)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket disconnect"))
	err = rep_socket.Unbind(TCP_ENDPOINT)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket unbind"))
}

func (ctx *SocketSuite) benchmarkSendReceive(c *check.C, sizeData int, endpoint string) {
	data := make([]byte, sizeData)

	rep_socket, err := ctx.NewSocket(PULL)
	if err != nil { panic(err) }
	req_socket, err := ctx.NewSocket(PUSH)
	if err != nil { panic(err) }

	err = rep_socket.Bind(endpoint)
	if err != nil { panic(err) }
	err = req_socket.Connect(endpoint)
	if err != nil { panic(err) }

	c.ResetTimer()
	var rep *MessagePart
	for i := 0; i < c.N; i++ {
		err := req_socket.Send(data, 0)
		if err != nil {
			panic(err)
		}
		rep, err = rep_socket.Recv(0)
		if err != nil {
			panic(err)
		}
		err = rep.FreeMsg()
		if err != nil { panic(err) }
	}
	c.StopTimer()

	req_socket.Disconnect(endpoint)
	rep_socket.Unbind(endpoint)

	err = rep_socket.Close()
	if err != nil { panic(err) }
	err = req_socket.Close()
	if err != nil { panic(err) }
}

func (ctx *SocketSuite) Benchmark1BSendReceiveTcp(c *check.C) {
	ctx.benchmarkSendReceive(c, 1, TCP_ENDPOINT)
}

func (ctx *SocketSuite) Benchmark1KBSendReceiveTcp(c *check.C) {
	ctx.benchmarkSendReceive(c, 1e3, TCP_ENDPOINT)
}

func (ctx *SocketSuite) Benchmark1MBSendReceiveTcp(c *check.C) {
	ctx.benchmarkSendReceive(c, 1e6, TCP_ENDPOINT)
}

func (ctx *SocketSuite) Benchmark1BSendReceiveInproc(c *check.C) {
	ctx.benchmarkSendReceive(c, 1, INPROC_ENDPOINT + "_1b")
}

func (ctx *SocketSuite) Benchmark1KBSendReceiveInproc(c *check.C) {
	ctx.benchmarkSendReceive(c, 1e3, INPROC_ENDPOINT + "_1K")
}

func (ctx *SocketSuite) Benchmark1MBSendReceiveInproc(c *check.C) {
	ctx.benchmarkSendReceive(c, 1e6, INPROC_ENDPOINT + "_1M")
}
