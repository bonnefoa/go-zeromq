package zmq

/*
#cgo pkg-config: libzmq
#include <zmq.h>
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"
)

type Context struct {
	c unsafe.Pointer
}


type SocketType C.int
const (
	REQ    = SocketType(C.ZMQ_REQ)
	REP    = SocketType(C.ZMQ_REP)
	ROUTER = SocketType(C.ZMQ_ROUTER)
	DEALER = SocketType(C.ZMQ_DEALER)
	PULL   = SocketType(C.ZMQ_PULL)
	PUSH   = SocketType(C.ZMQ_PUSH)
)

// Create a new thread safe context
func NewContext() (ctx *Context, err error) {
	ctx = &Context{}
	ctx.c, err = C.zmq_ctx_new()
	if ctx == nil {
		return nil, err
	}
	return ctx, nil
}

// Destroy a context.
// Don't forget to close all sockets before otherwise this call
// will hang forever
func (ctx *Context) Destroy() error {
	rc, err := C.zmq_ctx_destroy(ctx.c)
	if rc == -1 {
		return err
	}
	return nil
}

// Create a new socket
func (ctx *Context) NewSocket(socketType SocketType) (*Socket, error) {
	s, err := C.zmq_socket(ctx.c, C.int(socketType))
	socket := &Socket{s}
	if s == nil {
		return nil, err
	}
	return socket, nil
}
