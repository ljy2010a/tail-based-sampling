package main

import (
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/common"
)

//
//import (
//	"fmt"
//
//	"github.com/smartystreets-prototypes/go-disruptor"
//)
//
//func main() {
//	myDisruptor := disruptor.New(
//		disruptor.WithCapacity(BufferSize),
//		disruptor.WithConsumerGroup(MyConsumer{}))
//
//	go publish(myDisruptor)
//
//	myDisruptor.Read()
//}
//
//func publish(myDisruptor disruptor.Disruptor) {
//	for sequence := int64(0); sequence <= Iterations; {
//		sequence = myDisruptor.Reserve(Reservations)
//
//		for lower := sequence - Reservations + 1; lower <= sequence; lower++ {
//			ringBuffer[lower&BufferMask] = lower
//		}
//
//		myDisruptor.Commit(sequence-Reservations+1, sequence)
//	}
//
//	_ = myDisruptor.Close()
//}
//
//// ////////////////////
//
//type MyConsumer struct{}
//
//func (this MyConsumer) Consume(lower, upper int64) {
//	for ; lower <= upper; lower++ {
//		message := ringBuffer[lower&BufferMask]
//		if message != lower {
//			panic(fmt.Errorf("race condition: %d %d", message, lower))
//		}
//	}
//}
//
//const (
//	BufferSize   = 1024 * 64
//	BufferMask   = BufferSize - 1
//	Iterations   = 128 * 1024 * 32
//	Reservations = 1
//)
//
//var ringBuffer = [BufferSize][]byte{}

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

	tdmap := common.NewTDataMap()
	tdmap.LoadOrStore("1",&common.TData{Status:100})
	fmt.Println(tdmap.Load("1"))
}
