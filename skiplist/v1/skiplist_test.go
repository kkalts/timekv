package skiplist

import (
	"fmt"
	"testing"
)

func TestSkipListAdd(t *testing.T) {
	//for i := 0; i <100 ; i++ {
	//	time.Sleep(time.Millisecond*50)
	//	skipList := NewSkipList()
	//	var data1 = KVData{
	//		Key:   []byte("testKey1"),
	//		Value: []byte("value1"),
	//	}
	//	var data3 = KVData{
	//		Key:   []byte("testKey3"),
	//		Value: []byte("value3"),
	//	}
	//	var data2 = KVData{
	//		Key:   []byte("testKey2"),
	//		Value: []byte("value2"),
	//	}
	//	var data4 = KVData{
	//		Key:   []byte("testKey4"),
	//		Value: []byte("value4"),
	//	}
	//	var data5 = KVData{
	//		Key:   []byte("testKey5"),
	//		Value: []byte("value5"),
	//	}
	//	skipList.Add(data1)
	//	skipList.Add(data3)
	//	skipList.Add(data5)
	//	skipList.Add(data2)
	//	skipList.Add(data4)
	//	fmt.Println("sl.level=",skipList.level)
	//	//find := skipList.Find([]byte("testKey4"))
	//	//fmt.Println("find=",string(find))
	//}

	//skipList := NewSkipList()
	//
	//var data1 = KVData{
	//	Key:   []byte("testKey1"),
	//	Value: []byte("value1"),
	//}
	//var data3 = KVData{
	//	Key:   []byte("testKey3"),
	//	Value: []byte("value3"),
	//}
	//var data2 = KVData{
	//	Key:   []byte("testKey2"),
	//	Value: []byte("value2"),
	//}
	//var data4 = KVData{
	//	Key:   []byte("testKey4"),
	//	Value: []byte("value4"),
	//}
	//var data5 = KVData{
	//	Key:   []byte("testKey5"),
	//	Value: []byte("value5"),
	//}
	//var data6 = KVData{
	//	Key:   []byte("testKey5"),
	//	Value: []byte("value6"),
	//}
	//skipList.Add(data1)
	//skipList.Add(data3)
	//skipList.Add(data5)
	//skipList.Add(data2)
	//skipList.Add(data4)
	//skipList.Add(data6)
	//fmt.Println("sl.level=", skipList.level)

	//skipList2.Add2(data1)
	//skipList2.Add2(data3)
	//skipList2.Add2(data5)
	//skipList2.Add2(data2)
	//skipList2.Add2(data4)
	//skipList2.Add2(data6)
	//fmt.Println("skipList2.level=", skipList2.level)
	////fmt.Println("find=",string(skipList.Find([]byte("testKey5"))))
	//
	//fmt.Println("find=", string(skipList.Find([]byte("testKey5"))))
	//
	//PrintSkipList(skipList)
	//
	//sortB, curS, preS := CheckSkipListSort(skipList)
	//fmt.Printf("%v 当前KEY=%s 前一个KEY=%s \n",sortB,curS,preS)
	//
	//compare := bytes.Compare([]byte("testKey2"), []byte("testKey4"))
	//fmt.Println("compare",compare)

	skipList2 := NewSkipList()
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

	sortB, _, _ := CheckSkipListSortInt(skipList2)
	fmt.Printf("%v \n", sortB)

	find := skipList2.Find(IntToBytes(3))
	fmt.Println("find=", BytesToInt(find))

}
func TestSkipListFind(t *testing.T) {
	//skipList := NewSkipList()

}

func TestDelete(t *testing.T) {
	skipList2 := NewSkipList()
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
	// 删除节点
	skipList2.Delete(IntToBytes(3))
	//skipList2.Delete(IntToBytes(1))
	fmt.Println("---------------删除后--------------------")
	PrintSkipListInt(skipList2)
	var data8 = KVData{
		Key:   IntToBytes(8),
		Value: IntToBytes(8),
	}
	skipList2.Add(data8)
	fmt.Println("---------------增加后--------------------")
	PrintSkipListInt(skipList2)

	skipList2.Delete(IntToBytes(2))
	//skipList2.Delete(IntToBytes(1))
	fmt.Println("---------------删除后--------------------")
	PrintSkipListInt(skipList2)
}

//func RandString(len int) string {
//	bytes := make([]byte, len)
//	for i := 0; i < len; i++ {
//		b := r.Intn(26) + 65
//		bytes[i] = byte(b)
//	}
//	return string(bytes)
//}
//
//func TestSkipListBasicCRUD(t *testing.T) {
//	list := NewSkiplist(1000)
//
//	//Put & Get
//	entry1 := NewEntry([]byte(RandString(10)), []byte("Val1"))
//	list.Add(entry1)
//	vs := list.Search(entry1.Key)
//	assert.Equal(t, entry1.Value, vs.Value)
//
//	entry2 := NewEntry([]byte(RandString(10)), []byte("Val2"))
//	list.Add(entry2)
//	vs = list.Search(entry2.Key)
//	assert.Equal(t, entry2.Value, vs.Value)
//
//	//Get a not exist entry
//	assert.Nil(t, list.Search([]byte(RandString(10))).Value)
//
//	//Update a entry
//	entry2_new := NewEntry(entry1.Key, []byte("Val1+1"))
//	list.Add(entry2_new)
//	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)
//}
//
//func Benchmark_SkipListBasicCRUD(b *testing.B) {
//	list := NewSkiplist(100000000)
//	key, val := "", ""
//	maxTime := 1000
//	for i := 0; i < maxTime; i++ {
//		//number := rand.Intn(10000)
//		key, val = RandString(10), fmt.Sprintf("Val%d", i)
//		entry := NewEntry([]byte(key), []byte(val))
//		list.Add(entry)
//		searchVal := list.Search([]byte(key))
//		assert.Equal(b, searchVal.Value, []byte(val))
//	}
//}
//
//func TestDrawList(t *testing.T) {
//	list := NewSkiplist(1000)
//	n := 12
//	for i:=0; i<n; i++ {
//		index := strconv.Itoa(r.Intn(90)+10)
//		key := index + RandString(8)
//		entryRand := NewEntry([]byte(key), []byte(index))
//		list.Add(entryRand)
//	}
//	list.Draw(true)
//	fmt.Println(strings.Repeat("*", 30) + "分割线" + strings.Repeat("*", 30))
//	list.Draw(false)
//}
//
//func TestConcurrentBasic(t *testing.T) {
//	const n = 1000
//	l := NewSkiplist(100000000)
//	var wg sync.WaitGroup
//	key := func(i int) []byte {
//		return []byte(fmt.Sprintf("Keykeykey%05d", i))
//	}
//	for i := 0; i < n; i++ {
//		wg.Add(1)
//		go func(i int) {
//			defer wg.Done()
//			l.Add(NewEntry(key(i), key(i)))
//		}(i)
//	}
//	wg.Wait()
//
//	// Check values. Concurrent reads.
//	for i := 0; i < n; i++ {
//		wg.Add(1)
//		go func(i int) {
//			defer wg.Done()
//			v := l.Search(key(i))
//			require.EqualValues(t, key(i), v.Value)
//			return
//
//			require.Nil(t, v)
//		}(i)
//	}
//	wg.Wait()
//}
//
//func Benchmark_ConcurrentBasic(b *testing.B) {
//	const n = 1000
//	l := NewSkiplist(100000000)
//	var wg sync.WaitGroup
//	key := func(i int) []byte {
//		return []byte(fmt.Sprintf("keykeykey%05d", i))
//	}
//	for i := 0; i < n; i++ {
//		wg.Add(1)
//		go func(i int) {
//			defer wg.Done()
//			l.Add(NewEntry(key(i), key(i)))
//		}(i)
//	}
//	wg.Wait()
//
//	// Check values. Concurrent reads.
//	for i := 0; i < n; i++ {
//		wg.Add(1)
//		go func(i int) {
//			defer wg.Done()
//			v := l.Search(key(i))
//			require.EqualValues(b, key(i), v.Value)
//			require.NotNil(b, v)
//		}(i)
//	}
//	wg.Wait()
//}
//
//func TestSkipListIterator(t *testing.T) {
//	list := NewSkiplist(100000)
//
//	//Put & Get
//	entry1 := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
//	list.Add(entry1)
//	assert.Equal(t, entry1.Value, list.Search(entry1.Key).Value)
//
//	entry2 := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
//	list.Add(entry2)
//	assert.Equal(t, entry2.Value, list.Search(entry2.Key).Value)
//
//	//Update a entry
//	entry2_new := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
//	list.Add(entry2_new)
//	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)
//
//	iter := list.NewSkipListIterator()
//	for iter.Rewind(); iter.Valid(); iter.Next() {
//		fmt.Printf("iter key %s, value %s", iter.Item().Entry().Key, iter.Item().Entry().Value)
//	}
//}
