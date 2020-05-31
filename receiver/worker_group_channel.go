package receiver

import (
	"bufio"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

type ChannelGroupConsume struct {
	receiver     *Receiver
	logger       *zap.Logger
	lineChan     chan [][]byte
	lineGroupNum int
	doneFunc     func()
	doneOnce     sync.Once
	doneWg       sync.WaitGroup
	groupNum     int
	workNum      int
}

func NewChannelGroupConsume(receiver *Receiver, f func()) *ChannelGroupConsume {
	// 300w = 10.67s , 13.15s
	// 350w = 9.76s , 12.73s
	return &ChannelGroupConsume{
		receiver:     receiver,
		logger:       receiver.logger,
		lineChan:     make(chan [][]byte, 7000),
		lineGroupNum: 500,
		groupNum:     5000,
		workNum:      2,
		doneFunc:     f,
	}
}

func (c *ChannelGroupConsume) Read(rd io.Reader) {
	c.logger.Info("read start")
	//defer func() {
	//	err := recover()
	//	if err != nil {
	//		c.logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
	//	}
	//}()
	btime := time.Now()
	br := bufio.NewReaderSize(rd, 8192)
	size := 0
	total := 0
	//go func() {
	//	c.logger.Info("read stat",
	//		zap.Int("total", total),
	//	)
	//	time.Sleep(10 * time.Second)
	//}()
	i := 0
	lines := make([][]byte, c.lineGroupNum)
	for {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		size += len(line)
		total++
		nline := make([]byte, len(line))
		copy(nline, line)
		if i < c.lineGroupNum {
			lines[i] = nline
		}
		if i == c.lineGroupNum-1 {
			c.lineChan <- lines
			lines = make([][]byte, c.lineGroupNum)
			i = 0
			continue
		}
		i++
	}

	if i != 0 {
		c.lineChan <- lines[:i]
	}
	c.receiver.logger.Info("read file done ",
		zap.Int("total", total),
		zap.Int("sourceSize", size),
		zap.Duration("cost", time.Since(btime)),
	)
	btime = time.Now()
	close(c.lineChan)
	c.doneWg.Wait()
	c.logger.Info("consumer all done",
		zap.Duration("cost", time.Since(btime)),
	)
	c.doneFunc()
}

func (c *ChannelGroupConsume) StartConsume() {
	for i := 0; i < c.workNum; i++ {
		go c.consume()
	}
}

func (c *ChannelGroupConsume) consume() {
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
			zap.Int("size", size),
		)
		time.Sleep(10 * time.Second)
	}()
	once := sync.Once{}
	for lines := range c.lineChan {
		once.Do(func() {
			btime = time.Now()
		})
		for _, line := range lines {
			size++
			spanData := common.ParseSpanData(line)
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
