package receiver

import (
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

type ChannelGroupConsume struct {
	receiver     *Receiver
	logger       *zap.Logger
	lineChan     chan []int
	lineGroupNum int
	readBufSize  int
	readDoneFunc func()
	overFunc     func()
	doneOnce     sync.Once
	doneWg       sync.WaitGroup
	workNum      int
	blockLen     int
	lineBlock    []byte
	scannerBlock []byte
	posSlice     chan []int
}

func NewChannelGroupConsume(receiver *Receiver, readDone func(), over func()) *ChannelGroupConsume {
	// 500w = 1450MB
	blockLen := int(2.5 * 1024 * 1024 * 1024)
	readBufSize := 128 * 1024 * 1024
	c := &ChannelGroupConsume{
		receiver:     receiver,
		logger:       receiver.logger,
		lineChan:     make(chan []int, 40),
		lineGroupNum: 250000,
		readBufSize:  readBufSize,
		workNum:      2,
		readDoneFunc: readDone,
		overFunc:     over,
		blockLen:     blockLen,
		lineBlock:    make([]byte, blockLen),
		posSlice:     make(chan []int, 80),
		//scannerBlock: make([]byte, blockLen),
	}
	for i := 0; i < 80; i++ {
		c.posSlice <- make([]int, c.lineGroupNum)
	}
	return c
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
	size := 0
	total := 0
	maxLine := 0
	minLine := 0
	i := 0
	iLimit := c.lineGroupNum - 1
	lines := make([]int, c.lineGroupNum)

	//scanner := bufio.NewScanner(rd)
	//scanner.Buffer(c.scannerBlock, c.readBufSize)
	//scanner.Split(bufio.ScanLines)
	//for scanner.Scan() {
	//	line := scanner.Bytes()
	//	lLen := len(line)
	//	if pos+lLen > c.blockLen {
	//		pos = 0
	//	}
	//	copy(c.lineBlock[pos:], line)
	//	lines[i] = c.lineBlock[pos : pos+lLen]
	//	pos += lLen
	//	if i == iLimit {
	//		c.lineChan <- lines
	//		lines = make([][]byte, c.lineGroupNum)
	//		i = 0
	//		continue
	//	}
	//	i++
	//}

	br := NewReaderSize(rd, c.blockLen, c.readBufSize, c.lineBlock)
	//br := bufio.NewReaderSize(rd, c.readBufSize)
	for {
		//line, err := br.ReadSlice('\n')
		start, llen, err := br.ReadSlicePos('\n')
		if err != nil {
			c.logger.Info("err", zap.Error(err))
			break
		}
		//size += len(line)
		//total++
		//lLen := len(line)

		//lines[i] = line
		lines[i] = start<<16 | llen

		if i == iLimit {
			c.lineChan <- lines
			select {
			case lines = <-c.posSlice:
			default:
				lines = make([]int, c.lineGroupNum)
			}
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
	clen := len(c.lineChan)
	c.readDoneFunc()
	close(c.lineChan)
	c.doneWg.Wait()
	c.logger.Info("consumer all done",
		zap.Duration("read cost", rtime),
		zap.Duration("less cost", time.Since(ctime)),
		zap.Duration("total cost", time.Since(btime)),
		zap.Int("less count", clen),
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
	//defer func() {
	//	err := recover()
	//	if err != nil {
	//		c.logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
	//	}
	//}()
	c.doneWg.Add(1)
	defer c.doneWg.Done()

	btime := time.Now()
	size := 0
	wrong := 0
	//once := sync.Once{}
	idToSpans := make(map[string]*TData, 1024)
	for lines := range c.lineChan {
		//once.Do(func() {
		//	btime = time.Now()
		//})
		//size += len(lines)
		c.receiver.ConsumeByte(lines, idToSpans)
		for k := range idToSpans {
			delete(idToSpans, k)
		}
	}
	c.logger.Info("deal file done ",
		zap.Int("wrong", wrong),
		zap.Int("dealSize", size),
		zap.Duration("cost", time.Since(btime)),
	)
}
