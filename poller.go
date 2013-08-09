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

const WAIT_FOREVER = C.long(-1)

const (
	POLLIN = PollEvent(C.ZMQ_POLLIN)
	POLLOUT = PollEvent(C.ZMQ_POLLOUT)
	POLLERR = PollEvent(C.ZMQ_POLLERR)
)

type ZmqPollItem C.zmq_pollitem_t
type PollItems []*PollItem
type PollItem struct {
	Socket *Socket
	Events PollEvent
	REvents PollEvent
}

// Build a zmq poll item from socket and
func (p *PollItem) buildZmqPollItem() ZmqPollItem {
	zmqItem := ZmqPollItem {
		socket : p.Socket.psocket,
		events : C.short(p.Events),
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
	var msTimeout C.long
	if timeout < 0 {
		msTimeout = WAIT_FOREVER
	} else {
		msTimeout = C.long(timeout.Nanoseconds() / 1e6)
	}
	sizeItems := C.int(len(p))
	zmqItems := p.buildZmqPollItems()
	rc, err := C.zmq_poll((*C.zmq_pollitem_t)(&zmqItems[0]), sizeItems, msTimeout)
	count := int(rc)
	if count == -1 {
		return count, err
	}
	for i := range p {
		p[i].REvents = PollEvent(zmqItems[i].revents)
	}
	return count, nil
}
