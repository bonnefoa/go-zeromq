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

