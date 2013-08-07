package zerogo

import (
	check "launchpad.net/gocheck"
	"testing"
)

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
	err := s.Destroy()
	c.Assert(err, check.IsNil, check.Commentf("Error on context destruction"))
}

func (ctx *SocketSuite) TestSocketCreate(c *check.C) {
	socket, err := ctx.NewSocket(REQ)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket creation"))
	err = socket.Close()
	c.Assert(err, check.IsNil, check.Commentf("Error on socket closing"))
}

func (ctx *SocketSuite) TestSocketTcpBind(c *check.C) {
	endpoint := "tcp://127.0.0.1:5555"

	socket, err := ctx.NewSocket(ROUTER)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket creation"))
	defer socket.Close()

	err = socket.Bind(endpoint)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket bind"))
	err = socket.Unbind(endpoint)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket unbind"))
}

func (ctx *SocketSuite) TestSocketSend(c *check.C) {
	endpoint := "tcp://127.0.0.1:9999"
	data := []byte("test")

	rep_socket, err := ctx.NewSocket(REP)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket creation"))
	defer rep_socket.Close()

	req_socket, err := ctx.NewSocket(REQ)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket creation"))
	defer req_socket.Close()

	err = rep_socket.Bind(endpoint)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket bind"))

	err = req_socket.Connect(endpoint)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket bind"))

	err = req_socket.Send(data, 0)
	c.Assert(err, check.IsNil, check.Commentf("Error on send"))
	response, err := rep_socket.Recv(0)

	c.Assert(err, check.IsNil, check.Commentf("Error on receive"))
	c.Assert(response, check.DeepEquals, data)
	err = rep_socket.Send(response, 0)
	c.Assert(err, check.IsNil, check.Commentf("Error on response send"))
	response, err = req_socket.Recv(0)
	c.Assert(err, check.IsNil, check.Commentf("Error on response receive"))
	c.Assert(response, check.DeepEquals, data)

	err = req_socket.Disconnect(endpoint)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket disconnect"))
	err = rep_socket.Unbind(endpoint)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket unbind"))
}


func (ctx *SocketSuite) BenchmarkSocket(c *check.C) {
	endpoint := "tcp://127.0.0.1:9999"
	data := []byte("test")

	rep_socket, _ := ctx.NewSocket(PUSH)
	req_socket, _ := ctx.NewSocket(PULL)

	rep_socket.Bind(endpoint)

	req_socket.Connect(endpoint)

	for i := 0; i < c.N; i++{
		err := req_socket.Send(data, 0)
		if err != nil { continue }
		_, err = rep_socket.Recv(0)
		if err != nil { continue }
	}

	req_socket.Disconnect(endpoint)
	rep_socket.Unbind(endpoint)

	rep_socket.Close()
	req_socket.Close()
}
