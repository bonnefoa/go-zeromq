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
	rc, err := ctx.Get(IoThreads)
	if err != nil {
		t.Fatal("Error on context get of IoThreads", err)
	}
	if rc <= 0 {
		t.Fatal("expected IoThreads to be > 0, got ", rc)
	}
	ctx.Destroy()
}

func TestContextSet(t *testing.T) {
	ctx, _ := NewContext()
	err := ctx.Set(IoThreads, 4)
	if err != nil {
		t.Fatal("Error on context set of IoThreads", err)
	}
	count, err := ctx.Get(IoThreads)
	if count != 4 {
		t.Fatalf("Expected IoThreads options to be 4, got %q (err was %q)", count, err)
	}
	ctx.Destroy()
}
