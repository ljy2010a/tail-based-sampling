package receiver

import (
	"bufio"
	"fmt"
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
	readBufSize  int
	readDoneFunc func()
	overFunc     func()
	doneOnce     sync.Once
	doneWg       sync.WaitGroup
	//groupNum     int
	workNum int
}

func NewChannelGroupConsume(receiver *Receiver, readDone func(), over func()) *ChannelGroupConsume {
	// 300w = 10.67s , 13.15s
	// 350w = 9.76s , 12.73s
	// 500w = 1450MB , 6.98s , 9.52s  7.33 , 9.21
	return &ChannelGroupConsume{
		receiver:     receiver,
		logger:       receiver.logger,
		lineChan:     make(chan [][]byte, 1300),
		lineGroupNum: 5000,
		//groupNum:     5000,
		readBufSize:  16 * 1024,
		workNum:      2,
		readDoneFunc: readDone,
		overFunc:     over,
	}
}

func (c *ChannelGroupConsume) Read(rd io.Reader) {
	c.logger.Info("read start")
	defer func() {
		err := recover()
		if err != nil {
			c.logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
		}
	}()
	btime := time.Now()
	br := bufio.NewReaderSize(rd, c.readBufSize)
	size := 0
	total := 0
	//go func() {
	//	c.logger.Info("read stat",
	//		zap.Int("total", total),
	//	)
	//	time.Sleep(10 * time.Second)
	//}()
	maxLine := 0
	minLine := 0
	i := 0
	lines := make([][]byte, c.lineGroupNum)
	for {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		size += len(line)
		//lLen := len(line)
		//if maxLine < lLen {
		//	maxLine = lLen
		//}
		//if minLine > lLen || minLine == 0 {
		//	minLine = lLen
		//}
		total++
		//bb := bytebufferpool.Get()
		//bb.Write(line)
		//lines[i] = bb.Bytes()
		//var nline []byte
		//if lLen <= 200 {
		//	nline = c.receiver.p200.Get().([]byte)
		//}
		//if lLen <= 300 {
		//	nline = c.receiver.p300.Get().([]byte)
		//} else {
		//	nline = c.receiver.p400.Get().([]byte)
		//}
		nline := make([]byte, len(line))
		copy(nline, line)
		lines[i] = nline
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
	rtime := time.Since(btime)
	ctime := time.Now()
	c.readDoneFunc()
	close(c.lineChan)
	c.doneWg.Wait()
	c.logger.Info("consumer all done",
		zap.Duration("read cost", rtime),
		zap.Duration("less cost", time.Since(ctime)),
		zap.Duration("total cost", time.Since(btime)),
		zap.Int("maxLine", maxLine),
		zap.Int("minLine", minLine),
		zap.Int("total", total),
		zap.Int("sourceSize", size),
	)
	c.overFunc()
}

func (c *ChannelGroupConsume) StartConsume() {
	for i := 0; i < c.workNum; i++ {
		go c.consume()
	}
}

func (c *ChannelGroupConsume) consume() {
	defer func() {
		err := recover()
		if err != nil {
			c.logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
		}
	}()
	c.doneWg.Add(1)
	defer c.doneWg.Done()

	btime := time.Now()
	size := 0
	wrong := 0
	//spanDatas := make([]*common.SpanData, c.groupNum)
	//i := 0
	//go func() {
	//	c.logger.Info("read stat",
	//		zap.Int("wrong", wrong),
	//		zap.Int("size", size),
	//	)
	//	time.Sleep(10 * time.Second)
	//}()
	once := sync.Once{}
	for lines := range c.lineChan {
		once.Do(func() {
			btime = time.Now()
		})
		size += len(lines)
		c.receiver.ConsumeByte(lines)
		//for _, line := range lines {
		//	size++
		//	if len(line) < 60 {
		//		continue
		//	}
		//	spanData := c.receiver.ParseSpanData(line)
		//	if spanData == nil {
		//		continue
		//	}
		//	if spanData.Wrong {
		//		wrong++
		//	}
		//	if i < c.groupNum {
		//		spanDatas[i] = spanData
		//	}
		//	if i == c.groupNum-1 {
		//		c.receiver.ConsumeTraceData(spanDatas)
		//		i = 0
		//		continue
		//	}
		//	i++
		//}
	}
	//if i != 0 {
	//	c.receiver.ConsumeTraceData(spanDatas[:i])
	//}
	c.logger.Info("deal file done ",
		zap.Int("wrong", wrong),
		zap.Int("dealSize", size),
		zap.Duration("cost", time.Since(btime)),
	)
}
