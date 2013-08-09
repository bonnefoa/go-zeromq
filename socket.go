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

func (socType SocketType) IsBind() bool {
	switch socType {
	case REP:
		return true
	case ROUTER:
		return true
	case DEALER:
		return true
	case PULL:
		return true
	case PUB:
		return true
	case XPUB:
		return true
	default:
		return false
	}
}

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
type SocketOptionString C.int

const (
	TYPE                    = SocketOptionInt(C.ZMQ_TYPE)
	RCVMORE                 = SocketOptionInt(C.ZMQ_RCVMORE)
	SNDHWM                  = SocketOptionInt(C.ZMQ_SNDHWM)
	RCVHWM                  = SocketOptionInt(C.ZMQ_RCVHWM)
	AFFINITY                = SocketOptionUint64(C.ZMQ_AFFINITY)
	IDENTITY                = SocketOptionString(C.ZMQ_IDENTITY)
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

	SUBSCRIBE		= SocketOptionString(C.ZMQ_SUBSCRIBE)
	UNSUBSCRIBE             = SocketOptionString(C.ZMQ_UNSUBSCRIBE)
	ROUTER_MANDATORY        = SocketOptionInt(C.ZMQ_ROUTER_MANDATORY)
	XPUB_VERBOSE            = SocketOptionInt(C.ZMQ_XPUB_VERBOSE)
)

func (s *Socket) getOption(option C.int, v interface{}, size *C.size_t) error {
	value := reflect.ValueOf(v)
	pvalue := unsafe.Pointer(value.Pointer())
	rc, err := C.zmq_getsockopt(s.psocket, option, pvalue, size)
	if rc == -1 {
		return err
	}
	return nil
}

// Get the value of a socket option as an int
func (s *Socket) GetOptionInt(option SocketOptionInt) (int, error) {
	var value int
	size := C.size_t(unsafe.Sizeof(value))
	err := s.getOption(C.int(option), &value, &size)
	return value, err
}

// Get the value of a socket option as an uint64
func (s *Socket) GetOptionUint64(option SocketOptionUint64) (uint64, error) {
	var value uint64
	size := C.size_t(unsafe.Sizeof(value))
	err := s.getOption(C.int(option), &value, &size)
	return value, err
}

// Get the value of a socket option as an int64
func (s *Socket) GetOptionInt64(option SocketOptionUint64) (int64, error) {
	var value int64
	size := C.size_t(unsafe.Sizeof(value))
	err := s.getOption(C.int(option), &value, &size)
	return value, err
}

// Get the value of a socket option as a string
func (s *Socket) GetOptionString(option SocketOptionString) (string, error) {
	var value [1024]byte
	sizeString := C.size_t(unsafe.Sizeof(value))
	err := s.getOption(C.int(option), &value, &sizeString)
	if sizeString > 0 {
		// Remove \x00 from zmq string
		return string(value[:sizeString-1]), err
	}
	return "", nil
}

func (s *Socket) setOption(option C.int, v interface{}, size C.size_t) error {
	value := reflect.ValueOf(v)
	pvalue := unsafe.Pointer(value.Pointer())
	rc, err := C.zmq_setsockopt(s.psocket, option, pvalue, size)
	if rc == -1 {
		return err
	}
	return nil
}

// Set a int socket option to the given value
func (s *Socket) SetOptionInt(option SocketOptionInt, value int) error {
	val := C.int(value)
	size := C.size_t(unsafe.Sizeof(val))
	return s.setOption(C.int(option), &val, size)
}

// Set a int 64 socket option to the given value
func (s *Socket) SetOptionInt64(option SocketOptionInt64, value int64) error {
	size := C.size_t(unsafe.Sizeof(value))
	return s.setOption(C.int(option), &value, size)
}

// Set a uint 64 socket option to the given value
func (s *Socket) SetOptionUint64(option SocketOptionUint64, value uint64) error {
	size := C.size_t(unsafe.Sizeof(value))
	return s.setOption(C.int(option), &value, size)
}

// Set a string socket option to the given value. Can be nil
func (s *Socket) SetOptionString(option SocketOptionString, value *string) error {
	if value == nil {
		return s.setOption(C.int(option), nil, 0)
	}
	size := C.size_t(len(*value))
	cstr := C.CString(*value)
	err := s.setOption(C.int(option), cstr, size)
	C.free(unsafe.Pointer(cstr))
	return err
}
