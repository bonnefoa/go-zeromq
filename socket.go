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

// Socket represents a zero mq socket
type Socket struct {
	psocket unsafe.Pointer
}

// SocketType identifies the type of the socket
type SocketType C.int

// Bindings to available socket types
const (
	Req    = SocketType(C.ZMQ_REQ)
	Rep    = SocketType(C.ZMQ_REP)
	Router = SocketType(C.ZMQ_ROUTER)
	Dealer = SocketType(C.ZMQ_DEALER)
	Pull   = SocketType(C.ZMQ_PULL)
	Push   = SocketType(C.ZMQ_PUSH)
	Pub    = SocketType(C.ZMQ_PUB)
	Sub    = SocketType(C.ZMQ_SUB)
	Xsub   = SocketType(C.ZMQ_XSUB)
	Xpub   = SocketType(C.ZMQ_XPUB)
	Pair   = SocketType(C.ZMQ_PAIR)
)

// SendFlag identifies the flags passed to zeromq send command
type SendFlag C.int

// Bindings to available send flags
const (
	SndMore  = SendFlag(C.ZMQ_SNDMORE)
	DontWait = SendFlag(C.ZMQ_DONTWAIT)
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
	var pdata unsafe.Pointer
	var msg C.zmq_msg_t
	if len(data) == 0 {
		pdata = unsafe.Pointer(&data)
	} else {
		pdata = unsafe.Pointer(&data[0])
	}

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

// SendMultipart sends a message with on or several frames to the socket
func (s *Socket) SendMultipart(data [][]byte, flag SendFlag) error {
	moreFlag := flag | SndMore
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

// RecvMultipart receives a multi part message from the socket
func (s *Socket) RecvMultipart(flag SendFlag) (*MessageMultipart, error) {
	msg := &MessageMultipart{}
	msg.parts = make([]*MessagePart, 0, 10)
	i := 0
	for {
		msgPart, err := s.Recv(flag)
		if err != nil {
			return nil, err
		}
		msg.parts = append(msg.parts, msgPart)
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

// Recv receives a message part from the socket
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
	msgPart.zmqMsg = (*zmqMsg)(&msg)

	return msgPart, nil
}

// SocketOptionInt identifies socket option which returns int value
type SocketOptionInt C.int
// SocketOptionUint64 identifies socket option which returns uint64 value
type SocketOptionUint64 C.int
// SocketOptionInt64 identifies socket option which returns int64 value
type SocketOptionInt64 C.int
// SocketOptionString identifies socket option which returns string value
type SocketOptionString C.int

// Bindings to socket options
const (
	Type                 = SocketOptionInt(C.ZMQ_TYPE)
	Rcvmore              = SocketOptionInt(C.ZMQ_RCVMORE)
	Sndhwm               = SocketOptionInt(C.ZMQ_SNDHWM)
	Rcvhwm               = SocketOptionInt(C.ZMQ_RCVHWM)
	Affinity             = SocketOptionUint64(C.ZMQ_AFFINITY)
	Identity             = SocketOptionString(C.ZMQ_IDENTITY)
	Rate                 = SocketOptionInt(C.ZMQ_RATE)
	RecoveryIvl          = SocketOptionInt(C.ZMQ_RECOVERY_IVL)
	Sndbuf               = SocketOptionInt(C.ZMQ_SNDBUF)
	Rcvbuf               = SocketOptionInt(C.ZMQ_RCVBUF)
	Linger               = SocketOptionInt(C.ZMQ_LINGER)
	ReconnectIvl         = SocketOptionInt(C.ZMQ_RECONNECT_IVL)
	ReconnectIvlMax      = SocketOptionInt(C.ZMQ_RECONNECT_IVL_MAX)
	Backlog              = SocketOptionInt(C.ZMQ_BACKLOG)
	Maxmsgsize           = SocketOptionInt64(C.ZMQ_MAXMSGSIZE)
	MulticastHops        = SocketOptionInt(C.ZMQ_MULTICAST_HOPS)
	Rcvtimeo             = SocketOptionInt(C.ZMQ_RCVTIMEO)
	Sndtimeo             = SocketOptionInt(C.ZMQ_SNDTIMEO)
	Ipv4only             = SocketOptionInt(C.ZMQ_IPV4ONLY)
	DelayAttachOnConnect = SocketOptionInt(C.ZMQ_DELAY_ATTACH_ON_CONNECT)
	Fd                   = SocketOptionInt(C.ZMQ_FD)
	Events               = SocketOptionInt(C.ZMQ_EVENTS)
	LastEndpoint         = SocketOptionString(C.ZMQ_LAST_ENDPOINT)
	TcpKeepalive         = SocketOptionInt(C.ZMQ_TCP_KEEPALIVE)
	TcpKeepaliveIdle     = SocketOptionInt(C.ZMQ_TCP_KEEPALIVE_IDLE)
	TcpKeepaliveCnt      = SocketOptionInt(C.ZMQ_TCP_KEEPALIVE_CNT)
	TcpKeepaliveIntvl    = SocketOptionInt(C.ZMQ_TCP_KEEPALIVE_INTVL)

	Subscribe       = SocketOptionString(C.ZMQ_SUBSCRIBE)
	Unsubscribe     = SocketOptionString(C.ZMQ_UNSUBSCRIBE)
	RouterMandatory = SocketOptionInt(C.ZMQ_ROUTER_MANDATORY)
	XpubVerbose     = SocketOptionInt(C.ZMQ_XPUB_VERBOSE)
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

// GetOptionInt gets the value of a socket option as an int
func (s *Socket) GetOptionInt(option SocketOptionInt) (int, error) {
	var value int
	size := C.size_t(unsafe.Sizeof(value))
	err := s.getOption(C.int(option), &value, &size)
	return value, err
}

// GetOptionUint64 gets the value of a socket option as an uint64
func (s *Socket) GetOptionUint64(option SocketOptionUint64) (uint64, error) {
	var value uint64
	size := C.size_t(unsafe.Sizeof(value))
	err := s.getOption(C.int(option), &value, &size)
	return value, err
}

// GetOptionInt64 gets the value of a socket option as an int64
func (s *Socket) GetOptionInt64(option SocketOptionUint64) (int64, error) {
	var value int64
	size := C.size_t(unsafe.Sizeof(value))
	err := s.getOption(C.int(option), &value, &size)
	return value, err
}

// GetOptionString gets the value of a socket option as a string
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

// SetOptionInt sets a int socket option to the given value
func (s *Socket) SetOptionInt(option SocketOptionInt, value int) error {
	val := C.int(value)
	size := C.size_t(unsafe.Sizeof(val))
	return s.setOption(C.int(option), &val, size)
}

// SetOptionInt64 sets a int 64 socket option to the given value
func (s *Socket) SetOptionInt64(option SocketOptionInt64, value int64) error {
	size := C.size_t(unsafe.Sizeof(value))
	return s.setOption(C.int(option), &value, size)
}

// SetOptionUint64 sets a uint 64 socket option to the given value
func (s *Socket) SetOptionUint64(option SocketOptionUint64, value uint64) error {
	size := C.size_t(unsafe.Sizeof(value))
	return s.setOption(C.int(option), &value, size)
}

// SetOptionString sets a string socket option to the given value. Can be nil
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

// SocketEvent identifies socket events available
type SocketEvent C.int

// Bindings to socket events
const (
	EventConnected        = SocketEvent(C.ZMQ_EVENT_CONNECTED)
	EventConnectDelayed  = SocketEvent(C.ZMQ_EVENT_CONNECT_DELAYED)
	EventConnectRetried = SocketEvent(C.ZMQ_EVENT_CONNECT_RETRIED)
	EventListening        = SocketEvent(C.ZMQ_EVENT_LISTENING)
	EventBindFailed      = SocketEvent(C.ZMQ_EVENT_BIND_FAILED)
	EventAccepted         = SocketEvent(C.ZMQ_EVENT_ACCEPTED)
	EventAcceptFailed    = SocketEvent(C.ZMQ_EVENT_ACCEPT_FAILED)
	EventClosed           = SocketEvent(C.ZMQ_EVENT_CLOSED)
	EventCloseFailed     = SocketEvent(C.ZMQ_EVENT_CLOSE_FAILED)
	EventDisconnected     = SocketEvent(C.ZMQ_EVENT_DISCONNECTED)
	EventAll              = SocketEvent(C.ZMQ_EVENT_ALL)
)

// Monitor binds event to the socket
func (s *Socket) Monitor(endpoint string, events SocketEvent) error {
	cstr := C.CString(endpoint)
	rc, err := C.zmq_socket_monitor(s.psocket, cstr, C.int(events))
	if rc == -1 {
		return err
	}
	return nil
}
