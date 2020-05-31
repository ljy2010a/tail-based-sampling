package main

import (
	"fmt"

	"github.com/smartystreets-prototypes/go-disruptor"
)

func main() {
	myDisruptor := disruptor.New(
		disruptor.WithCapacity(BufferSize),
		disruptor.WithConsumerGroup(MyConsumer{}))

	go publish(myDisruptor)

	myDisruptor.Read()
}

func publish(myDisruptor disruptor.Disruptor) {
	for sequence := int64(0); sequence <= Iterations; {
		sequence = myDisruptor.Reserve(Reservations)

		for lower := sequence - Reservations + 1; lower <= sequence; lower++ {
			ringBuffer[lower&BufferMask] = lower
		}

		myDisruptor.Commit(sequence-Reservations+1, sequence)
	}

	_ = myDisruptor.Close()
}

// ////////////////////

type MyConsumer struct{}

func (this MyConsumer) Consume(lower, upper int64) {
	for ; lower <= upper; lower++ {
		message := ringBuffer[lower&BufferMask]
		if message != lower {
			panic(fmt.Errorf("race condition: %d %d", message, lower))
		}
	}
}

const (
	BufferSize   = 1024 * 64
	BufferMask   = BufferSize - 1
	Iterations   = 128 * 1024 * 32
	Reservations = 1
)

var ringBuffer = [BufferSize][]byte{}