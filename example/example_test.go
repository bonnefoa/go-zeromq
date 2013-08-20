package zmq_test

import (
	"fmt"
	zmq "github.com/bonnefoa/go-zeromq"
	"log"
)

const testEndpoint = "tcp://127.0.0.1:4567"

func reqSocket(ctx *zmq.Context) *zmq.Socket {
	soc, err := ctx.NewSocket(zmq.Req)
	if err != nil {
		log.Fatal(err)
	}
	err = soc.Connect(testEndpoint)
	if err != nil {
		log.Fatal(err)
	}
	return soc
}

// Create a simple loop for response socket
// which send back received messages
func loopRep(soc *zmq.Socket) {
	for {
		msg, err := soc.Recv(0)
		if err != nil {
			return
		}
		err = soc.Send(msg.Data, 0)
		if err != nil {
			return
		}
		// Message needs to be close after use
		// to avoid memory leak
		msg.Close()
	}
}

func repSocket(ctx *zmq.Context) *zmq.Socket {
	soc, err := ctx.NewSocket(zmq.Rep)
	if err != nil {
		log.Fatal(err)
	}
	err = soc.Bind(testEndpoint)
	if err != nil {
		log.Fatal(err)
	}
	return soc
}

func ExampleSimpleRepReq() {
	// Create context
	ctx, err := zmq.NewContext()
	if err != nil {
		log.Fatal(err)
	}
	// Create request socket
	req := reqSocket(ctx)
	// Create response socket
	rep := repSocket(ctx)
	go loopRep(rep)

	// Send request
	err = req.Send([]byte("test"), 0)
	if err != nil {
		log.Fatal(err)
	}
	msg, err := req.Recv(0)
	if err != nil {
		log.Fatal(err)
	}

	// Message needs to be close after use
	// to avoid memory leak
	msg.Close()

	// Close sockets
	req.Close()
	rep.Close()

	// Destroy context
	// If a socket is still alive, destroy will hang up
	ctx.Destroy()

	fmt.Printf("%s", msg.Data)
	// Output: test
}
