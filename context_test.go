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

func TestContextSet(t *testing.T) {
	ctx, _ := NewContext()
	err := ctx.Set(IO_THREADS, 4)
	if err != nil {
		t.Fatal("Error on context set of IO_THREADS", err)
	}
	count, err := ctx.Get(IO_THREADS)
	if count != 4 {
		t.Fatalf("Expected IO_THREADS options to be 4, got %q (err was %q)", count, err)
	}
	ctx.Destroy()
}
