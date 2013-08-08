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

type ZmqMsg C.zmq_msg_t

type MessageMultipart struct {
	Data [][]byte
	msg  []*ZmqMsg
}

type MessagePart struct {
	Data []byte
	*ZmqMsg
}

// Close all zmq messages to release data and memory
func (m *MessageMultipart) Close() error {
	for _, v := range m.msg {
		err := v.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close zmq message to release data and memory
func (m *ZmqMsg) Close() error {
	rc, err := C.zmq_msg_close((*C.zmq_msg_t)(m))
	if rc == -1 {
		return err
	}
	return nil
}

// Check if there are more message to fetch
func (m *ZmqMsg) HasMore() bool {
	cint := C.zmq_msg_more((*C.zmq_msg_t)(m))
	if cint == C.int(1) {
		return true
	}
	return false
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
