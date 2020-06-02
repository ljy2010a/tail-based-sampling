package receiver

import (
	"bufio"
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

type ChannelConsume struct {
	receiver *Receiver
	logger   *zap.Logger
	lineChan chan []byte
	doneFunc func()
	doneOnce sync.Once
	doneWg   sync.WaitGroup
	groupNum int
	workNum  int
}

func NewChannelConsume(receiver *Receiver, f func()) *ChannelConsume {
	return &ChannelConsume{
		receiver: receiver,
		logger:   receiver.logger,
		lineChan: make(chan []byte, 50000),
		groupNum: 5000,
		workNum:  2,
		doneFunc: f,
	}
}

func (c *ChannelConsume) Read(rd io.Reader) {
	defer func() {
		err := recover()
		if err != nil {
			c.logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
		}
	}()
	btime := time.Now()
	br := bufio.NewReaderSize(rd, 4096)
	size := 0
	total := 0
	go func() {
		c.logger.Info("read stat",
			zap.Int("total", total),
		)
		time.Sleep(10 * time.Second)
	}()
	for {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		size += len(line)
		total++
		nline := make([]byte, len(line))
		copy(nline, line)
		c.lineChan <- nline
	}
	c.receiver.logger.Info("read file done ",
		zap.Int("total", total),
		zap.Int("sourceSize", size),
		zap.Duration("cost", time.Since(btime)),
	)
	close(c.lineChan)
	c.doneWg.Wait()
	c.logger.Info("consumer all done")
	c.doneFunc()
}

func (c *ChannelConsume) StartConsume() {
	for i := 0; i < c.workNum; i++ {
		go c.consume()
	}
}

func (c *ChannelConsume) consume() {
	c.doneWg.Add(1)
	defer c.doneWg.Done()

	btime := time.Now()
	size := 0
	wrong := 0
	spanDatas := make([]*common.SpanData, c.groupNum)
	i := 0
	go func() {
		c.logger.Info("read stat",
			zap.Int("wrong", wrong),
		)
		time.Sleep(10 * time.Second)
	}()
	once := sync.Once{}
	for line := range c.lineChan {
		once.Do(func() {
			btime = time.Now()
		})
		size++
		spanData := c.receiver.ParseSpanData(line)
		if spanData == nil {
			continue
		}
		if spanData.Wrong {
			wrong++
		}
		if i < c.groupNum {
			spanDatas[i] = spanData
		}
		if i == c.groupNum-1 {
			c.receiver.ConsumeTraceData(spanDatas)
			i = 0
			continue
		}
		i++
	}
	if i != 0 {
		c.receiver.ConsumeTraceData(spanDatas[:i])
	}
	c.logger.Info("deal file done ",
		zap.Int("wrong", wrong),
		zap.Int("dealSize", size),
		zap.Duration("cost", time.Since(btime)),
	)
}
