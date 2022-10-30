package v1

import (
	"fmt"
	"testing"
)

func TestCmRow(t *testing.T) {
	bytes := newCmRow(17)

	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	get1 := bytes.get(1)
	fmt.Println(get1)

	bytes.increment(1)
	bytes.increment(2)
	bytes.increment(4)

	get2 := bytes.get(1)

	get3 := bytes.get(4)
	fmt.Println(get2)
	fmt.Println(get3)

	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	bytes.increment(1)
	get4 := bytes.get(1)
	fmt.Println(get4)
	bytes.increment(0)
	bytes.increment(0)
	bytes.increment(0)
	get5 := bytes.get(0)
	fmt.Println(get5)
	bytes.increment(9)
	bytes.increment(9)
	get6 := bytes.get(9)
	fmt.Println(get6)

	bytes.reset()
	get7 := bytes.get(9)
	fmt.Println(get7)

	get8 := bytes.get(1)
	fmt.Println(get8)

	bytes.clear()
	fmt.Println(bytes.get(1))

	fmt.Println("--------------------------")

	TestCmRow2(t)
}

func TestCmRow2(t *testing.T) {

	bytes := newCmRow(16)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	get1 := bytes.get2(1)
	fmt.Println(get1)

	bytes.increment2(1)
	bytes.increment2(2)
	bytes.increment2(4)

	get2 := bytes.get2(1)

	get3 := bytes.get2(4)
	fmt.Println(get2)
	fmt.Println(get3)

	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	bytes.increment2(1)
	get4 := bytes.get2(1)
	fmt.Println(get4)
	bytes.increment2(0)
	bytes.increment2(0)
	bytes.increment2(0)
	get5 := bytes.get2(0)
	fmt.Println(get5)
	bytes.increment2(9)
	bytes.increment2(9)
	get6 := bytes.get2(9)
	fmt.Println(get6)

	bytes.reset()
	get7 := bytes.get2(9)
	fmt.Println(get7)

	get8 := bytes.get2(1)
	fmt.Println(get8)
}

func TestCmSketch(t *testing.T) {
	sketch := NewCmSketch(18)
	sketch.Increment(9)
	sketch.Increment(9)
	sketch.Increment(9)
	sketch.Increment(9)
	fmt.Println(sketch.Estimate(9))
	sketch.Reset()
	fmt.Println(sketch.Estimate(9))
	sketch.Clear()
	fmt.Println(sketch.Estimate(9))
}
