package v1

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestNodeSize(t *testing.T) {
	nodeSize := uint32(unsafe.Sizeof(node{}))
	fmt.Println(nodeSize)
}
