package zerogo

/*
#cgo pkg-config: libzmq
#include <zmq.h>
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"unsafe"
)

type ZeromqError int
type SocketType C.int

const (
	REQ    = SocketType(C.ZMQ_REQ)
	REP    = SocketType(C.ZMQ_REP)
	ROUTER = SocketType(C.ZMQ_ROUTER)
	DEALER = SocketType(C.ZMQ_DEALER)
	PULL   = SocketType(C.ZMQ_PULL)
	PUSH   = SocketType(C.ZMQ_PUSH)
)

func (z ZeromqError) Error() string {
	return string(z)
}

type Context struct {
	c unsafe.Pointer
}

type Socket struct {
	c unsafe.Pointer
}

func getZmqError() error {
	return ZeromqError(C.zmq_errno())
}

func checkReturnType(r C.int) error {
	if r == -1 {
		return getZmqError()
	}
	return nil
}

func NewContext() (ctx *Context, err error) {
	ctx = &Context{}
	ctx.c = C.zmq_ctx_new()
	if ctx.c == nil {
		err = getZmqError()
	}
	return
}

func (ctx *Context) Destroy() error {
	r := C.zmq_ctx_destroy(ctx.c)
	return checkReturnType(r)
}

func (ctx *Context) NewSocket(socketType SocketType) (socket *Socket, err error) {
	socket = &Socket{}
	socket.c = C.zmq_socket(ctx.c, C.int(socketType))
	if socket.c == nil {
		err = getZmqError()
	}
	return
}

func (soc *Socket) Close() (err error) {
	r := C.zmq_close(soc.c)
	return checkReturnType(r)
}

func (soc *Socket) Bind(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	r := C.zmq_bind(soc.c, addr)
	return checkReturnType(r)
}

func (soc *Socket) Unbind(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	r := C.zmq_bind(soc.c, addr)
	return checkReturnType(r)
}
