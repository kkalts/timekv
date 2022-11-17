package v1

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func RandString(len int) string {
	bytes := make([]byte, len)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < len; i++ {
		b := rand.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func TestTestSkipListBasicCRUD(t *testing.T) {
	for i := 0; i < 10; i++ {
		TestSkipListBasicCRUD(t)
		time.Sleep(time.Duration(1) * time.Second)
	}

}
func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList(1000)
	key1 := RandString(12)
	key2 := RandString(16)
	key3 := RandString(17)
	key4 := RandString(15)
	fmt.Println(key1)
	fmt.Println(key2)
	fmt.Println(key3)
	fmt.Println(key4)
	//Put & Get
	entry1 := NewEntry([]byte(key1), []byte("Val1"))
	list.Add(entry1)
	vs := list.Get(entry1.Key)
	assert.Equal(t, entry1.Value, vs.Value)
	list.Draw(true)

	entry2 := NewEntry([]byte(key2), []byte("Val2"))
	list.Add(entry2)
	vs = list.Get(entry2.Key)
	assert.Equal(t, entry2.Value, vs.Value)
	list.Draw(true)

	entry3 := NewEntry([]byte(key3), []byte("Val3"))
	list.Add(entry3)
	vs = list.Get(entry3.Key)
	assert.Equal(t, entry3.Value, vs.Value)
	list.Draw(true)

	//Get a not exist entry
	assert.Nil(t, list.Get([]byte(key4)).Value)

	//Update a entry
	entry2_new := NewEntry(entry1.Key, []byte("Val1+1"))
	list.Add(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Get(entry2_new.Key).Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkipList(100000000)
	key, val := "", ""
	maxTime := 1000
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = RandString(10), fmt.Sprintf("Val%d", i)
		entry := NewEntry([]byte(key), []byte(val))
		list.Add(entry)
		searchVal := list.Get([]byte(key))
		assert.Equal(b, searchVal.Value, []byte(val))
	}
}

func TestDrawList(t *testing.T) {
	list := NewSkipList(1000)
	n := 12
	for i := 0; i < n; i++ {
		index := strconv.Itoa(rand.Intn(90) + 10)
		key := index + RandString(8)
		entryRand := NewEntry([]byte(key), []byte(index))
		list.Add(entryRand)
	}
	list.Draw(true)
	fmt.Println(strings.Repeat("*", 30) + "分割线" + strings.Repeat("*", 30))
	list.Draw(false)
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkipList(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("Keykeykey%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Get(key(i))
			require.EqualValues(t, key(i), v.Value)
			return

			require.Nil(t, v)
		}(i)
	}
	wg.Wait()
	l.Draw(true)
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 1000
	l := NewSkipList(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("keykeykey%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Get(key(i))
			require.EqualValues(b, key(i), v.Value)
			require.NotNil(b, v)
		}(i)
	}
	wg.Wait()
}
func TestSkipListIterator2(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			TestSkipListIterator(t)
		}(i)
	}
	wg.Wait()
	//for i := 0; i <100 ; i++ {
	//	TestSkipListIterator(t)
	//}

}
func TestSkipListIterator(t *testing.T) {
	list := NewSkipList(100000)

	//Put & Get
	entry1 := NewEntry([]byte(RandString(10)), []byte(RandString(17)))
	list.Add(entry1)
	assert.Equal(t, entry1.Value, list.Get(entry1.Key).Value)

	entry2 := NewEntry([]byte(RandString(10)), []byte(RandString(18)))
	list.Add(entry2)
	assert.Equal(t, entry2.Value, list.Get(entry2.Key).Value)

	//Update a entry
	entry2_new := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Add(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Get(entry2_new.Key).Value)

	list.Draw(true)
	iter := list.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		fmt.Printf("iter key %s, value %s\n", iter.Item().Key, iter.Item().Value)
	}
	fmt.Println("-----------------------------------------")
}

func TestU32(t *testing.T) {
	//var data = make([]byte,0)
	// 声明的是切片
	var u32Slice = []uint32{1, 2, 3}
	fmt.Printf("u32的值:%v \n", u32Slice)
	fmt.Printf("u32的值:%p \n", u32Slice)
	fmt.Printf("&u32的:%p \n", &u32Slice)
	fmt.Printf("u32[0]的地址:%p \n", &u32Slice[0])
	fmt.Printf("u32[1]的地址:%p \n", &u32Slice[1])
	//
	//var intArr [3]int
	//fmt.Println(intArr)
	//intArr[0] = 10
	//intArr[1] = 20
	//intArr[2] = 30
	//fmt.Printf("intArr的地址=%p int[0]地址=%p int[1]地址%p int[2]地址%p", &intArr, &intArr[0], &intArr[1], &intArr[2])

	// 声明数组

	bytes := U32SliceToBytes(u32Slice)
	fmt.Println("bytes", bytes)
	fmt.Println(len(bytes))
	fmt.Printf("bytes的地址%p \n", &bytes)
}

func U32SliceToBytes(data []uint32) []byte {
	if len(data) == 0 {
		return nil
	}
	var b []byte

	// 通过反射 将引用变成指针
	// 强转为*reflect.SliceHeader
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	//hdr := *(*reflect.SliceHeader)(unsafe.Pointer(&b))
	fmt.Printf("b的地址:%p \n", &b)
	fmt.Printf("hdr的类型:%v \n", reflect.TypeOf(hdr))
	fmt.Printf("hdr的值:%p \n", hdr)
	//fmt.Printf("hdr的值2:%p \n",*hdr)
	fmt.Printf("hdr的地址:%p \n", &hdr)
	hdr.Len = len(data) * 4
	hdr.Cap = hdr.Len
	//fmt.Println("data的数据",data)
	// 为什么data的地址和data[0]的地址不同
	fmt.Printf("data的地址=%p \n", &data)
	fmt.Printf("data的地址=%p \n", &data[0])
	fmt.Printf("data的地址=%p \n", &data[1])
	fmt.Printf("data的地址=%p \n", &data[2])
	// data切片就是第一个元素的地址
	// hdr.Data 存储data[0]的地址
	hdr.Data = uintptr(unsafe.Pointer(&data[0]))
	//fmt.Printf("hdr.Data的地址=%p \n",hdr.Data)
	fmt.Printf("hdr.Data =%v \n", hdr.Data)
	fmt.Printf("b的值%v\n", b)
	return b
}

func TestU32ToBytes(t *testing.T) {
	var v uint32 = 1
	bytes := U32ToBytes(v)
	fmt.Println(bytes)
}

/*
	u32转字节切片
*/
func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	// 大端序处理
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

func TestReflectFunc(t *testing.T) {
	call1 := func(v1 int, v2 int) {
		t.Log(v1, v2)
	}
	call2 := func(v1 int, v2 int, s string) {
		t.Log(v1, v2, s)
	}
	var (
		function reflect.Value
		inValue  []reflect.Value
		n        int
	)
	bridge := func(call interface{}, args ...interface{}) {
		n = len(args)
		inValue = make([]reflect.Value, n)
		for i := 0; i < n; i++ {
			inValue[i] = reflect.ValueOf(args[i])
		}
		function = reflect.ValueOf(call)
		function.Call(inValue)
	}
	bridge(call1, 1, 2)
	bridge(call2, 1, 2, "test2")
}

func TestSlice(t *testing.T) {
	fmt.Printf("Slice:\n")

	var a [10]byte

	// reflect.SliceHeader is a runtime representation of the internal workings
	// of a slice. To make it point to a specific address, use something like
	// the following:
	//    h.Data = uintptr(0x100)
	// And replace '0x100' with the desired address.
	var h reflect.SliceHeader
	h.Data = uintptr(unsafe.Pointer(&a)) // The address of the first element of the underlying array.
	h.Len = len(a)
	h.Cap = len(a)

	// Create an actual slice from the SliceHeader.
	//s := *(*[]byte)(unsafe.Pointer(&h))
	// 取*[]byte类型的值
	s := *(*[]byte)(unsafe.Pointer(&h))
	fmt.Printf("s的类型是%v\n", reflect.TypeOf(s))
	fmt.Printf("Before:\n\ts: %v\n\ta: %v\n", s, a)
	fmt.Printf("Before:\n\ts的地址: %p\n\ta的地址: %p\n", &s, &a)
	// Copy a string into the slice. This fills the underlying array, which in
	// this case has been manually set to 'a'.
	//copy(s, "A string.")
	a[0] = 1
	fmt.Printf("After:\n\ts: %v\n\ta: %v\n", s, a)
}
