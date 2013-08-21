package zmq

/*
#cgo pkg-config: libzmq
#include <zmq.h>
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"reflect"
	"runtime"
	"unsafe"
)

type zmqMsg C.zmq_msg_t

// MessageMultipart represents a multipart frame message
type MessageMultipart struct {
	parts []*MessagePart
	Data  [][]byte
}

// MessagePart represents a single message frame
type MessagePart struct {
	Data []byte
	*zmqMsg
}

func (m *MessageMultipart) aggregateData() {
	m.Data = make([][]byte, len(m.parts))
	for i, part := range m.parts {
		m.Data[i] = part.Data
	}
}

// Close all zmq messages to release data and memory
func (m *MessageMultipart) Close() error {
	var err error
	for _, part := range m.parts {
		cerr := part.Close()
		if err == nil {
			err = cerr
		}
	}
	if err != nil {
		return err
	}

	return nil
}

// Close zmq message and put back MessagePart to pool
func (m *MessagePart) Close() error {
	return m.zmqMsg.Close()
}

// Close zmq message to release data and memory
func (m *zmqMsg) Close() error {
	runtime.SetFinalizer(m, nil)
	rc, err := C.zmq_msg_close((*C.zmq_msg_t)(m))
	if rc == -1 {
		return err
	}
	return nil
}

// Check if there are more message to fetch
func (m *zmqMsg) HasMore() bool {
	cint := C.zmq_msg_more((*C.zmq_msg_t)(m))
	if cint == C.int(1) {
		return true
	}
	return false
}

// Get event of the given message
func (m *zmqMsg) GetEvent() *C.zmq_event_t {
	var event C.zmq_event_t
	sizeEvent := C.size_t(unsafe.Sizeof(event))
	pdata := C.zmq_msg_data((*C.zmq_msg_t)(m))
	C.memcpy(unsafe.Pointer(&event), pdata, sizeEvent)
	return &event
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
