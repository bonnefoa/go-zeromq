package zmq

import (
	"testing"
)

func TestContextCreateAndDestroy(t *testing.T) {
	ctx, err := NewContext()
	if err != nil {
		t.Fatal("Error on context creation", err)
	}
	err = ctx.Destroy()
	if err != nil {
		t.Fatal("Error on context destroy", err)
	}
}

func TestContextGet(t *testing.T) {
	ctx, _ := NewContext()
	rc, err := ctx.Get(IO_THREADS)
	if err != nil {
		t.Fatal("Error on context get of IO_THREADS", err)
	}
	if rc <= 0 {
		t.Fatal("expected IO_THREADS to be > 0, got ", rc)
	}
	ctx.Destroy()
}
