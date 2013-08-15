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

type pollEvent C.short

const waitForever = C.long(-1)

// Available polling events
const (
	Pollin = pollEvent(C.ZMQ_POLLIN)
	Pollout = pollEvent(C.ZMQ_POLLOUT)
	Pollerr = pollEvent(C.ZMQ_POLLERR)
)

type zmqPollItem C.zmq_pollitem_t

// PollItems agregates multiple poll events
type PollItems []*PollItem
// PollItem identifies a poll events to wait on a socket
type PollItem struct {
	Socket *Socket
	Events pollEvent
	REvents pollEvent
}

// Build a zmq poll item from socket and Event
func (p *PollItem) buildZmqPollItem() zmqPollItem {
	zmqItem := zmqPollItem {
		socket : p.Socket.psocket,
		events : C.short(p.Events),
	}
	return zmqItem
}

func (p PollItems) buildZmqPollItems() []zmqPollItem {
	zmqItems := make([]zmqPollItem, len(p))
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
		msTimeout = waitForever
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
		p[i].REvents = pollEvent(zmqItems[i].revents)
	}
	return count, nil
}
