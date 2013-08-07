package zerogo

/*
#cgo pkg-config: libzmq
#include <zmq.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
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

type SendFlag C.int

const (
	SNDMORE  = SendFlag(C.ZMQ_SNDMORE)
	DONTWAIT = SendFlag(C.ZMQ_DONTWAIT)
)

func (z ZeromqError) Error() string {
	return string(z)
}

type Context struct {
	c unsafe.Pointer
}

type Socket struct {
	s unsafe.Pointer
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

func (soc *Socket) Close() error {
	_, err := C.zmq_close(soc.s)
	return err
}

func (soc *Socket) Bind(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	r, err := C.zmq_bind(soc.s, addr)
	if r == 0 {
		return nil
	}
	return err
}

func (soc *Socket) Unbind(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	r, err := C.zmq_unbind(soc.s, addr)
	if r == 0 {
		return nil
	}
	return err
}

func (soc *Socket) Connect(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	r, err := C.zmq_connect(soc.s, addr)
	if r == 0 {
		return nil
	}
	return err
}

func (soc *Socket) Disconnect(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	r, err := C.zmq_disconnect(soc.s, addr)
	if r == 0 {
		return nil
	}
	return err
}

func (soc *Socket) Send(data []byte, flag SendFlag) error {
	pdata := unsafe.Pointer(&data[0])
	var msg C.zmq_msg_t;
	sizeData := C.size_t(len(data))
	C.zmq_msg_init_data(&msg, pdata, sizeData, nil, nil)
	r, err := C.zmq_msg_send(&msg, soc.s, C.int(0))
	C.zmq_msg_close(&msg)
	if r == -1 {
		return err
	}
	return nil
}

func (soc *Socket) Recv(flag SendFlag) ([]byte, error) {
	var msg C.zmq_msg_t;
	rc, err := C.zmq_msg_init(&msg);
	defer C.zmq_msg_close (&msg);
	if rc != 0 {
		return []byte{}, err
	}
	rc, err = C.zmq_msg_recv(&msg, soc.s, 0);
	if rc == -1 {
		return []byte{}, err
	}

	sizeMsg := C.int(C.zmq_msg_size(&msg))
	pdata := C.zmq_msg_data(&msg)
	return C.GoBytes(pdata, sizeMsg), nil
}
