package zmq

/*
#cgo pkg-config: libzmq
#include <zmq.h>
#include <stdlib.h>
*/
import "C"

import (
	"time"
)

type PollEvent C.short

const (
	POLLIN = PollEvent(C.ZMQ_POLLIN)
	POLLOUT = PollEvent(C.ZMQ_POLLOUT)
	POLLERR = PollEvent(C.ZMQ_POLLERR)
)

type ZmqPollItem C.zmq_pollitem_t
type PollItems []PollItem
type PollItem struct {
	Socket *Socket
	Events []PollEvent
}

func (p PollItem) buildZmqPollItem() ZmqPollItem {
	event := C.short(0)
	for _, ev := range p.Events {
		event |= (C.short)(ev)
	}
	zmqItem := ZmqPollItem {
		socket : p.Socket.psocket,
		events : event,
	}
	return zmqItem
}

func (p PollItems) buildZmqPollItems() []ZmqPollItem {
	zmqItems := make([]ZmqPollItem, len(p))
	for i, v := range p {
		zmqItems[i] = v.buildZmqPollItem()
	}
	return zmqItems
}

func (p PollItems) Poll(timeout time.Duration) (int, error) {
	msTimeout := C.long(timeout.Nanoseconds() / 1e6)
	sizeItems := C.int(len(p))
	zmqItems := p.buildZmqPollItems()
	rc, err := C.zmq_poll((*C.zmq_pollitem_t)(&zmqItems[0]), sizeItems, msTimeout)
	count := int(rc)
	if count == -1 {
		return count, err
	}
	return count, nil
}
