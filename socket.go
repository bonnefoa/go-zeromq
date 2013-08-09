package zmq

/*
#cgo pkg-config: libzmq
#include <zmq.h>
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"
	"reflect"
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
		rc, err := C.zmq_msg_send(&msg, s.psocket, C.int(flag))
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
	msg := &MessageMultipart{}
	msg.parts = make([]*MessagePart, 10)
	i := 0
	for {
		msgPart, err := s.Recv(flag)
		if err != nil {
			// Close fetched message before returing error
			msg.Close()
			return nil, err
		}
		msg.parts[i] = msgPart
		i += 1
		if !msgPart.HasMore() {
			break
		}
	}
	// Make slice iterable
	msg.parts = msg.parts[:i]
	msg.aggregateData()
	return msg, nil
}

var messagePartPool = make(chan *MessagePart, 100)

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

	var msgPart *MessagePart
	select {
	case msgPart = <-messagePartPool:
	default:
		msgPart = &MessagePart{}
	}

	msgPart.Data = data
	msgPart.ZmqMsg = (*ZmqMsg)(&msg)

	return msgPart, nil
}

type SocketOptionInt C.int
type SocketOptionUint64 C.int
type SocketOptionInt64 C.int
type SocketOptionBinary C.int
type SocketOptionString C.int

const (
	TYPE                    = SocketOptionInt(C.ZMQ_TYPE)
	RCVMORE                 = SocketOptionInt(C.ZMQ_RCVMORE)
	SNDHWM                  = SocketOptionInt(C.ZMQ_SNDHWM)
	RCVHWM                  = SocketOptionInt(C.ZMQ_RCVHWM)
	AFFINITY                = SocketOptionUint64(C.ZMQ_AFFINITY)
	IDENTITY                = SocketOptionBinary(C.ZMQ_IDENTITY)
	RATE                    = SocketOptionInt(C.ZMQ_RATE)
	RECOVERY_IVL            = SocketOptionInt(C.ZMQ_RECOVERY_IVL)
	SNDBUF                  = SocketOptionInt(C.ZMQ_SNDBUF)
	RCVBUF                  = SocketOptionInt(C.ZMQ_RCVBUF)
	LINGER                  = SocketOptionInt(C.ZMQ_LINGER)
	RECONNECT_IVL           = SocketOptionInt(C.ZMQ_RECONNECT_IVL)
	RECONNECT_IVL_MAX       = SocketOptionInt(C.ZMQ_RECONNECT_IVL_MAX)
	BACKLOG                 = SocketOptionInt(C.ZMQ_BACKLOG)
	MAXMSGSIZE              = SocketOptionInt64(C.ZMQ_MAXMSGSIZE)
	MULTICAST_HOPS          = SocketOptionInt(C.ZMQ_MULTICAST_HOPS)
	RCVTIMEO                = SocketOptionInt(C.ZMQ_RCVTIMEO)
	SNDTIMEO                = SocketOptionInt(C.ZMQ_SNDTIMEO)
	IPV4ONLY                = SocketOptionInt(C.ZMQ_IPV4ONLY)
	DELAY_ATTACH_ON_CONNECT = SocketOptionInt(C.ZMQ_DELAY_ATTACH_ON_CONNECT)
	FD                      = SocketOptionInt(C.ZMQ_FD)
	EVENTS                  = SocketOptionInt(C.ZMQ_EVENTS)
	LAST_ENDPOINT           = SocketOptionString(C.ZMQ_LAST_ENDPOINT)
	TCP_KEEPALIVE           = SocketOptionInt(C.ZMQ_TCP_KEEPALIVE)
	TCP_KEEPALIVE_IDLE      = SocketOptionInt(C.ZMQ_TCP_KEEPALIVE_IDLE)
	TCP_KEEPALIVE_CNT       = SocketOptionInt(C.ZMQ_TCP_KEEPALIVE_CNT)
	TCP_KEEPALIVE_INTVL     = SocketOptionInt(C.ZMQ_TCP_KEEPALIVE_INTVL)
)

func (s *Socket) getOption(option C.int, v interface{}) (int, error) {
	value := reflect.ValueOf(v)
	pvalue := unsafe.Pointer(value.Pointer())
	size := C.size_t(unsafe.Sizeof(reflect.Indirect(value)))
	rc, err := C.zmq_getsockopt(s.psocket, option, pvalue, &size)
	if rc == -1 {
		return -1, err
	}
	return int(size), nil
}

func (s *Socket) GetOptionInt(option SocketOptionInt) (int, error) {
	var value int
	_, err := s.getOption(C.int(option), &value)
	return value, err
}

func (s *Socket) GetOptionUint64(option SocketOptionUint64) (uint64, error) {
	var value uint64
	_, err := s.getOption(C.int(option), &value)
	return value, err
}

func (s *Socket) GetOptionInt64(option SocketOptionUint64) (int64, error) {
	var value int64
	_, err := s.getOption(C.int(option), &value)
	return value, err
}

func (s *Socket) GetOptionString(option SocketOptionString) (string, error) {
	var value [1024]byte
	sizeString, err := s.getOption(C.int(option), &value)
	if sizeString > 0 {
		// Remove \x00 from zmq string
		return string(value[:sizeString-1]), err
	}
	return "", nil
}

