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

func TestArenaSkipList(t *testing.T) {
	skipList2 := NewSkipList(1000)
	var data1 = KVData{
		Key:   IntToBytes(1),
		Value: IntToBytes(1),
	}
	var data3 = KVData{
		Key:   IntToBytes(3),
		Value: IntToBytes(3),
	}
	var data2 = KVData{
		Key:   IntToBytes(2),
		Value: IntToBytes(2),
	}
	var data4 = KVData{
		Key:   IntToBytes(4),
		Value: IntToBytes(4),
	}
	var data5 = KVData{
		Key:   IntToBytes(5),
		Value: IntToBytes(5),
	}
	var data6 = KVData{
		Key:   IntToBytes(6),
		Value: IntToBytes(6),
	}
	var data7 = KVData{
		Key:   IntToBytes(3),
		Value: IntToBytes(7),
	}
	skipList2.Add(data1)
	skipList2.Add(data3)
	skipList2.Add(data5)
	skipList2.Add(data2)
	skipList2.Add(data4)
	skipList2.Add(data6)
	skipList2.Add(data7)
	PrintSkipListInt(skipList2)

	// 删除
	skipList2.Delete(IntToBytes(3))

	fmt.Println("----------删除后---------------")
	PrintSkipListInt(skipList2)

	// 查找
	find3 := skipList2.Find(IntToBytes(3))
	fmt.Println("find3=", find3)
	find7 := skipList2.Find(IntToBytes(5))
	fmt.Println("find7=", BytesToInt(find7))
}
