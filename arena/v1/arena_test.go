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

func TestNodeAlign(t *testing.T) {
	nodeAlign := int(unsafe.Sizeof(uint64(0))) - 1
	fmt.Println("nA=", nodeAlign)
	var n uint32 = 10

	var height int = 1
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	fmt.Println("m=", m)

	nodeSize := int(unsafe.Sizeof(node{}))
	fmt.Println("nodeSize=", nodeSize)
	uint32Size := int(unsafe.Sizeof(uint32(0)))

	unusedSize := (maxHeight - height) * uint32Size
	fmt.Println("unusedSize=", unusedSize)
	fmt.Println("size=", nodeSize-unusedSize)

}
