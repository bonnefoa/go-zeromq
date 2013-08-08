package zmq

import (
	"testing"
)

func TestContextCreateAndDestroy(t *testing.T) {
	ctx, err := NewContext()
	if err != nil {
		t.Fatalf("Error on socket creation %v", err)
	}
	err = ctx.Destroy()
	if err != nil {
		t.Fatalf("Error on socket destruction %v", err)
	}
}
