package zmq

/*
#cgo pkg-config: libzmq
#include <zmq.h>
#include <stdlib.h>
*/
import "C"

import (
	"reflect"
	"unsafe"
)

type Socket struct {
	s unsafe.Pointer
}

type MessagePart struct {
	Data []byte
	msg  *C.zmq_msg_t
}

type SocketType C.int
type SendFlag C.int

const (
	REQ    = SocketType(C.ZMQ_REQ)
	REP    = SocketType(C.ZMQ_REP)
	ROUTER = SocketType(C.ZMQ_ROUTER)
	DEALER = SocketType(C.ZMQ_DEALER)
	PULL   = SocketType(C.ZMQ_PULL)
	PUSH   = SocketType(C.ZMQ_PUSH)
)

const (
	SNDMORE  = SendFlag(C.ZMQ_SNDMORE)
	DONTWAIT = SendFlag(C.ZMQ_DONTWAIT)
)

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
	var msg C.zmq_msg_t
	sizeData := C.size_t(len(data))
	C.zmq_msg_init_data(&msg, pdata, sizeData, nil, nil)
	for {
		r, err := C.zmq_msg_send(&msg, soc.s, C.int(0))
		if r == -1 && C.zmq_errno() == C.int(C.EINTR) {
			continue
		}
		if r == -1 {
			return err
		}
		break
	}
	C.zmq_msg_close(&msg)
	return nil
}

func buildSliceFromMsg(msg *C.zmq_msg_t) []byte {
	pdata := C.zmq_msg_data(msg)
	sizeMsg := int(C.zmq_msg_size(msg))
	var x []unsafe.Pointer
	s := (*reflect.SliceHeader)(unsafe.Pointer(&x))
	s.Data = uintptr(pdata)
	s.Len = sizeMsg
	s.Cap = sizeMsg
	return *(*[]byte)(unsafe.Pointer(&x))
}

func (recvMsg *MessagePart) FreeMsg() error {
	rc, err := C.zmq_msg_close(recvMsg.msg)
	if rc == -1 {
		return err
	}
	return nil
}

func (soc *Socket) Recv(flag SendFlag) (*MessagePart, error) {
	var msg C.zmq_msg_t
	rc, err := C.zmq_msg_init(&msg)
	if rc != 0 {
		return nil, err
	}
	for {
		rc, err = C.zmq_msg_recv(&msg, soc.s, 0)
		if rc == -1 && C.zmq_errno() == C.int(C.EINTR) {
			continue
		}
		if rc == -1 {
			C.zmq_msg_close(&msg)
			return nil, err
		}
		break
	}
	data := buildSliceFromMsg(&msg)
	recvMessage := &MessagePart{data, &msg}
	return recvMessage, nil
}
