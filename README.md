go-zeromq
=========

go-zeromq is a binding go for zeromq.

This binding implements zero-copy for better performance.
One downside is that the data is not managed by the garbage collector, you have to explicitly free the message once it is no more used.

Basic Usage
-----------

Import go-zeromq:

```go
import zmq "github.com/bonnefoa/go-zeromq"
```

Create a new Context:

```go
ctx, err := zmq.NewContext()
if err != nil {
    log.Fatal(err)
}
```

Create a new Socket and bind it:

```go
soc, err := ctx.NewSocket(zmq.Rep)
if err != nil {
    log.Fatal(err)
}
err = soc.Bind("tcp://*:4444")
if err != nil {
    log.Fatal(err)
}
```

Receive and send messages:

```go
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
```
