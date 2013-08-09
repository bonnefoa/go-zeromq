package zmq

/*
#cgo pkg-config: libzmq
#include <zmq.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
)

type SocketChannel struct {
	SendChannel chan [][]byte          // Channel for send message
	RecvChannel chan *MessageMultipart // Channel for recv message
	ErrChannel  chan error             // Channel for recv message
	Context     *Context
	Socket      *Socket // Socket connected to endpoint of type SocketType
	SocketType  SocketType
	Endpoint    string
	InSocket    *Socket // Socket used for inter thread communication
}

func (sc *SocketChannel) Close() {
	close(sc.SendChannel)
	close(sc.RecvChannel)
	sc.InSocket.Close()
	sc.Socket.Close()
	close(sc.ErrChannel)
}

func (sc *SocketChannel) sendRoutine(addr string) {
	in, err := sc.Context.NewSocket(PAIR)
	if err != nil {
		sc.ErrChannel <- err
		return
	}
	err = in.Connect(addr)
	if err != nil {
		sc.ErrChannel <- err
		return
	}
	for {
		toSend := <-sc.SendChannel
		if toSend == nil {
			// Channel is close, exiting
			in.Close()
			return
		}
		in.SendMultipart(toSend, 0)
	}
}

func (sc *SocketChannel) loopPolling() {
	socketItem := &PollItem{Socket: sc.Socket, Events: POLLIN}
	sendItem := &PollItem{Socket: sc.InSocket, Events: POLLIN}
	pollItems := PollItems{socketItem, sendItem}
	for {
		_, err := pollItems.Poll(-1)
		if err != nil {
			return
		}
		if socketItem.REvents == POLLIN {
			parts, err := sc.Socket.RecvMultipart(0)
			if err != nil {
				fmt.Println("Error on receive %s", err)
				return
			}
			sc.RecvChannel <- parts
		}
		if sendItem.REvents == POLLIN {
			msgParts, err := sc.InSocket.RecvMultipart(0)
			defer msgParts.Close()
			if err != nil {
				fmt.Println("Error on receive %s", err)
				return
			}
			err = sc.Socket.SendMultipart(msgParts.Data, 0)
			if err != nil {
				fmt.Println("Error on send %s", err)
				return
			}
		}
	}
}

func (sc *SocketChannel) createSocket() error {
	socket, err := sc.Context.NewSocket(sc.SocketType)
	if err != nil {
		return fmt.Errorf("New Socket error: %s", err)
	}
	sc.Socket = socket
	if sc.SocketType.IsBind() {
		err = socket.Bind(sc.Endpoint)
		if err != nil {
			return fmt.Errorf("Bind to %s error: %s", sc.Endpoint, err)
		}
	} else {
		err = socket.Connect(sc.Endpoint)
		if err != nil {
			return fmt.Errorf("Connect to %s error: %s", sc.Endpoint, err)
		}
	}
	return nil
}

func (sc *SocketChannel) createInprocSocket() error {
	var err error
	sc.InSocket, err = sc.Context.NewSocket(PAIR)
	if err != nil {
		return err
	}
	addr := "inproc://inter_" + sc.Endpoint + string(sc.SocketType)
	err = sc.InSocket.Bind(addr)
	if err != nil {
		return fmt.Errorf("Bind to %s error: %s", addr, err)
	}
	go sc.sendRoutine(addr)
	return err
}

func (sc *SocketChannel) wireSocket() error {
	runtime.LockOSThread()

	err := sc.createSocket()
	if err != nil {
		return fmt.Errorf("Socket creation error : %s", err)
	}
	err = sc.createInprocSocket()
	if err != nil {
		return fmt.Errorf("InProc creation error : %s", err)
	}

	go sc.loopPolling()
	return nil
}
