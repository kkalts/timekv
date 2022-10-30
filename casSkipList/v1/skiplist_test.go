package v1

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
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
