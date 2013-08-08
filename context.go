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

func NewContext() (ctx *Context, err error) {
	ctx = &Context{}
	ctx.c, err = C.zmq_ctx_new()
	return ctx, err
}

func (ctx *Context) Destroy() error {
	_, err := C.zmq_ctx_destroy(ctx.c)
	return err
}

func (ctx *Context) NewSocket(socketType SocketType) (*Socket, error) {
	s, err := C.zmq_socket(ctx.c, C.int(socketType))
	socket := &Socket{s}
	return socket, err
}
