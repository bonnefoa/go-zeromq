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

type SendFlag C.int

const (
	SNDMORE  = SendFlag(C.ZMQ_SNDMORE)
	DONTWAIT = SendFlag(C.ZMQ_DONTWAIT)
)

// Close 0mq socket.
func (soc *Socket) Close() error {
	rc, err := C.zmq_close(soc.s)
	if rc == 0 {
		return nil
	}
	return err
}

// Bind the socket to the given address
func (soc *Socket) Bind(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	rc, err := C.zmq_bind(soc.s, addr)
	if rc == 0 {
		return nil
	}
	return err
}

// Unbind the socket from the given address
func (soc *Socket) Unbind(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	rc, err := C.zmq_unbind(soc.s, addr)
	if rc == 0 {
		return nil
	}
	return err
}

// Connect the socket to the given address
func (soc *Socket) Connect(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	rc, err := C.zmq_connect(soc.s, addr)
	if rc == 0 {
		return nil
	}
	return err
}

// Disconnect the socket from the given address
func (soc *Socket) Disconnect(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	rc, err := C.zmq_disconnect(soc.s, addr)
	if rc == 0 {
		return nil
	}
	return err
}

// Send data to the socket
func (soc *Socket) Send(data []byte, flag SendFlag) error {
	var msg C.zmq_msg_t
	pdata := unsafe.Pointer(&data[0])
	sizeData := C.size_t(len(data))
	// The slice is reused as an unsafe pointer to avoid copy
	// There might be a problem if go gc collect data slice
	// before the message is effectively send.
	// TODO try to use a leaky bucket to mitigate gc collect
	// and improve performances
	C.zmq_msg_init_data(&msg, pdata, sizeData, nil, nil)
	for {
		rc, err := C.zmq_msg_send(&msg, soc.s, C.int(0))
		// Retry send on an interrupted system call
		if rc == -1 && C.zmq_errno() == C.int(C.EINTR) {
			continue
		}
		if rc == -1 {
			return err
		}
		break
	}
	C.zmq_msg_close(&msg)
	return nil
}

// Build a byte slice with content pointing to the message data
// The slice is manually build from the data pointer and message size.
// Since data is not managed by the gc, You need to call zmq_msg_close to free data
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

// Close zmq message to release data and memory
func (recvMsg *MessagePart) FreeMsg() error {
	rc, err := C.zmq_msg_close(recvMsg.msg)
	if rc == -1 {
		return err
	}
	return nil
}

// Receive a message part from the socket
// It is necessary to call FreeMsg on each MessagePart to avoid memory leak
// when the data is not needed anymore
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
