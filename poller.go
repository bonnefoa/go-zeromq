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

// Build a zmq poll item from socket and Event
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

// Poll until timeout or until one or multiple polled events happens
func (p PollItems) Poll(timeout time.Duration) (int, error) {
	var msTimeout C.long
    var rc C.int
    var err error
	if timeout < 0 {
		msTimeout = WAIT_FOREVER
	} else {
		msTimeout = C.long(timeout.Nanoseconds() / 1e6)
	}
	sizeItems := C.int(len(p))
	zmqItems := p.buildZmqPollItems()
    for {
        rc, err = C.zmq_poll((*C.zmq_pollitem_t)(&zmqItems[0]), sizeItems, msTimeout)
		if rc == -1 && C.zmq_errno() == C.int(C.EINTR) {
            continue
        }
        if rc == -1 {
            return -1, err
        }
        break
    }
	count := int(rc)
	for i := range p {
		p[i].REvents = PollEvent(zmqItems[i].revents)
	}
	return count, nil
}
