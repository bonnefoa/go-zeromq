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

type Socket struct {
	psocket unsafe.Pointer
}

type SocketType C.int

const (
	REQ    = SocketType(C.ZMQ_REQ)
	REP    = SocketType(C.ZMQ_REP)
	ROUTER = SocketType(C.ZMQ_ROUTER)
	DEALER = SocketType(C.ZMQ_DEALER)
	PULL   = SocketType(C.ZMQ_PULL)
	PUSH   = SocketType(C.ZMQ_PUSH)
	PUB    = SocketType(C.ZMQ_PUB)
	SUB    = SocketType(C.ZMQ_SUB)
	XSUB   = SocketType(C.ZMQ_XSUB)
	XPUB   = SocketType(C.ZMQ_XPUB)
	PAIR   = SocketType(C.ZMQ_PAIR)
)

type SendFlag C.int

const (
	SNDMORE  = SendFlag(C.ZMQ_SNDMORE)
	DONTWAIT = SendFlag(C.ZMQ_DONTWAIT)
)

// Close 0mq socket.
func (s *Socket) Close() error {
	rc, err := C.zmq_close(s.psocket)
	if rc == 0 {
		return nil
	}
	return err
}

// Bind the socket to the given address
func (s *Socket) Bind(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	rc, err := C.zmq_bind(s.psocket, addr)
	if rc == 0 {
		return nil
	}
	return err
}

// Unbind the socket from the given address
func (s *Socket) Unbind(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	rc, err := C.zmq_unbind(s.psocket, addr)
	if rc == 0 {
		return nil
	}
	return err
}

// Connect the socket to the given address
func (s *Socket) Connect(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	rc, err := C.zmq_connect(s.psocket, addr)
	if rc == 0 {
		return nil
	}
	return err
}

// Disconnect the socket from the given address
func (s *Socket) Disconnect(address string) error {
	addr := C.CString(address)
	defer C.free(unsafe.Pointer(addr))
	rc, err := C.zmq_disconnect(s.psocket, addr)
	if rc == 0 {
		return nil
	}
	return err
}

// Send data to the socket
func (s *Socket) Send(data []byte, flag SendFlag) error {
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
		rc, err := C.zmq_msg_send(&msg, s.psocket, C.int(0))
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

// Send multipart message to the socket
func (s *Socket) SendMultipart(data [][]byte, flag SendFlag) error {
	moreFlag := flag | SNDMORE
	for _, v := range data[:len(data)-1] {
		err := s.Send(v, moreFlag)
		if err != nil {
			return err
		}
	}
	err := s.Send(data[len(data)-1], flag)
	if err != nil {
		return err
	}
	return nil
}

// Receive a multi part message from the socket
func (s *Socket) RecvMultipart(flag SendFlag) (*MessageMultipart, error) {
	parts := make([][]byte, 0, 4)
	msgs := make([]*ZmqMsg, 0, 4)
	for {
		msgPart, err := s.Recv(flag)
		if err != nil {
			// Close fetched message before returing error
			for _, v := range msgs {
				v.CloseMsg()
			}
			return nil, err
		}
		parts = append(parts, msgPart.Data)
		msgs = append(msgs, msgPart.ZmqMsg)
		if !msgPart.HasMore(){
			break
		}
	}
	msgMpart := &MessageMultipart{parts, msgs}
	return msgMpart, nil
}

// Receive a message part from the socket
// It is necessary to call CloseMsg on each MessagePart to avoid memory leak
// when the data is not needed anymore
func (s *Socket) Recv(flag SendFlag) (*MessagePart, error) {
	var msg C.zmq_msg_t
	rc, err := C.zmq_msg_init(&msg)
	if rc != 0 {
		return nil, err
	}
	for {
		rc, err = C.zmq_msg_recv(&msg, s.psocket, 0)
		// Retry receive on an interrupted system call
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
	recvMessage := &MessagePart{data, (*ZmqMsg)(&msg)}
	return recvMessage, nil
}
