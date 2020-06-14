package main

import (
	"fmt"
)

const INT_MAX = int(^uint(0) >> 1)
const INT_MIN = ^INT_MAX

func main() {
	a := make([]int, 10)
	for i := 0; i < 10; i++ {
		a[i] = 1
	}
	fmt.Println(len(a), cap(a))
	fmt.Printf("%p\n", a)
	a = a[:0]
	fmt.Println(len(a), cap(a))
	fmt.Printf("%p\n", a)

	a = append(a, 1)
	fmt.Println(len(a), cap(a))
	fmt.Printf("%p\n", a)

	b := a[1:3]
	fmt.Println(len(b), cap(b))
	fmt.Printf("%p\n", b)

	fmt.Println(INT_MAX)
	//logger, _ := zap.NewProduction()
	var start int = 2147483648 + 100
	var end int = 109
	ret := start<<16 | end
	fmt.Println("ret", ret)
	fmt.Println("start", start)
	fmt.Println("end", end)
	start1 := ret >> 16
	fmt.Println("start1", start1)
	if start1 == start {
		fmt.Println("start ok")
	} else {
		fmt.Println("start wrong")
	}
	end1 := ret & 0xffff
	fmt.Println("end1", end1)
	if end1 == end {
		fmt.Println("end ok")
	} else {
		fmt.Println("end wrong")
	}

}
