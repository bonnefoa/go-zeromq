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

type ContextOption C.int
const (
	IO_THREADS = C.ZMQ_IO_THREADS
	MAX_SOCKETS = C.ZMQ_MAX_SOCKETS
)

type Context struct {
	c unsafe.Pointer
}

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

// Get context option value
func (ctx *Context) Get(option ContextOption) (int, error) {
	rc, err := C.zmq_ctx_get(ctx.c, C.int(option))
	count := int(rc)
	if count == -1 {
		return count, err
	}
	return count, nil
}

// Set context option to given value
func (ctx *Context) Set(option ContextOption, value int) error {
	rc, err := C.zmq_ctx_set(ctx.c, C.int(option), C.int(value))
	if rc == -1 {
		return err
	}
	return nil
}

