package zerogo

import (
	check "launchpad.net/gocheck"
	"testing"
)

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&ContextSuite{})
var _ = check.Suite(&SocketSuite{})

type ContextSuite struct{}

func (s *ContextSuite) TestContextCreateAndDestroy(c *check.C) {
	ctx, err := NewContext()
	c.Assert(err, check.IsNil, check.Commentf("Expected non nil context", err))
	err = ctx.Destroy()
	c.Assert(err, check.IsNil, check.Commentf("Error in context destruction", err))
}

type SocketSuite struct {
	*Context
}

func (s *SocketSuite) SetUpTest(c *check.C) {
	ctx, err := NewContext()
	c.Assert(err, check.IsNil, check.Commentf("Error on context creation, expected nil error", err))
	s.Context = ctx
}

func (s *SocketSuite) TearDownTest(c *check.C) {
	err := s.Destroy()
	c.Assert(err, check.IsNil, check.Commentf("Error on context destruction", err))
}

func (ctx *SocketSuite) TestSocketCreate(c *check.C) {
	socket, err := ctx.NewSocket(REQ)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket creation", err))
	err = socket.Close()
	c.Assert(err, check.IsNil, check.Commentf("Error on socket closing", err))
}

func (ctx *SocketSuite) TestSocketBind(c *check.C) {
	socket, err := ctx.NewSocket(REQ)
	c.Assert(err, check.IsNil, check.Commentf("Error on socket creation", err))
	err = socket.Close()
	c.Assert(err, check.IsNil, check.Commentf("Error on socket closing", err))
}
