package receiver

import (
	"github.com/ljy2010a/tailf-based-sampling/common"
	"sync"
	"time"
)

type DisruptorConsume struct {
	receiver  *Receiver
	btime     time.Time
	size      int64
	wrong     int64
	groupNum  int64
	i         int64
	spanDatas []*common.SpanData
	once      sync.Once
}

//
//const (
//	BufferSize   = 1024 * 64
//	BufferMask   = BufferSize - 1
//	Iterations   = 128 * 1024 * 32
//	Reservations = 1
//)
//
//var ringBuffer = [BufferSize][]byte{}

//myDisruptor := disruptor.New(
//	disruptor.WithCapacity(BufferSize),
//	disruptor.WithConsumerGroup())
//
//sequence = myDisruptor.Reserve(Reservations)
//
//for lower := sequence - Reservations + 1; lower <= sequence; lower++ {
//	ringBuffer[lower&BufferMask] = lower
//}
//
//myDisruptor.Commit(sequence-Reservations+1, sequence)

func NewDisruptorConsume(receiver *Receiver) *DisruptorConsume {
	return &DisruptorConsume{
		receiver:  receiver,
		btime:     time.Now(),
		groupNum:  5000,
		spanDatas: make([]*common.SpanData, 5000),
	}
}

//
//func (r *DisruptorConsume) ConsumeDisruptor(lower, upper int64) {
//	for ; lower <= upper; lower++ {
//		message := ringBuffer[lower&BufferMask]
//
//	}
//	i := 0
//	go func() {
//		r.receiver.logger.Info("read stat",
//			zap.Int("wrong", wrong),
//		)
//		time.Sleep(10 * time.Second)
//	}()
//	once := sync.Once{}
//	for line := range r.lineChan {
//		once.Do(func() {
//			btime = time.Now()
//		})
//		size++
//		spanData := common.ParseSpanData(line)
//		if spanData == nil {
//			continue
//		}
//		if spanData.Wrong {
//			wrong++
//		}
//		if i < groupNum {
//			spanDatas[i] = spanData
//		}
//		if i == groupNum-1 {
//			r.ConsumeTraceData(spanDatas)
//			i = 0
//			continue
//		}
//		i++
//	}
//	if i != 0 {
//		r.ConsumeTraceData(spanDatas[:i])
//	}
//	r.logger.Info("deal file done ",
//		zap.Int("wrong", wrong),
//		zap.Int("dealSize", size),
//		zap.Duration("cost", time.Since(btime)),
//	)
//	times := atomic.AddInt64(&r.closeTimes, 1)
//	if times == r.consumerLen {
//		close(r.finishChan)
//	}
//}
