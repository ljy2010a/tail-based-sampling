package receiver

import (
	"bufio"
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
	workNum      int
	blockLen     int
	lineBlock    []byte
	scannerBlock []byte
}

func NewChannelGroupConsume(receiver *Receiver, readDone func(), over func()) *ChannelGroupConsume {
	// 300w = 10.67s , 13.15s
	// 350w = 9.76s , 12.73s
	// 500w = 1450MB , 6.98s , 9.52s  7.33 , 9.21
	blockLen := int(1.5 * 1024 * 1024 * 1024)
	readBufSize := 256 * 1024 * 1024
	return &ChannelGroupConsume{
		receiver:     receiver,
		logger:       receiver.logger,
		lineChan:     make(chan [][]byte, 50),
		lineGroupNum: 40000,
		readBufSize:  readBufSize,
		workNum:      2,
		readDoneFunc: readDone,
		overFunc:     over,
		blockLen:     blockLen,
		lineBlock:    make([]byte, blockLen),
		//scannerBlock: make([]byte, readBufSize),
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
	br := bufio.NewReaderSize(rd, c.readBufSize)
	//scanner := bufio.NewScanner(rd)
	//scanner.Buffer(c.scannerBlock, c.readBufSize)
	//scanner.Split(bufio.ScanLines)
	size := 0
	total := 0
	maxLine := 0
	minLine := 0
	i := 0
	pos := 0
	lines := make([][]byte, c.lineGroupNum)

	//for scanner.Scan() {
	//	line := scanner.Bytes()
	//	lLen := len(line)
	//	if pos+lLen > c.blockLen {
	//		pos = 0
	//	}
	//	copy(c.lineBlock[pos:], line)
	//	lines[i] = c.lineBlock[pos : pos+lLen]
	//	pos += lLen
	//	if i == c.lineGroupNum-1 {
	//		c.lineChan <- lines
	//		lines = make([][]byte, c.lineGroupNum)
	//		i = 0
	//		continue
	//	}
	//	i++
	//}

	for {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		//size += len(line)
		lLen := len(line)
		//if maxLine < lLen {
		//	maxLine = lLen
		//}
		//if minLine > lLen || minLine == 0 {
		//	minLine = lLen
		//}
		//total++
		//nline := make([]byte, len(line))
		//copy(nline, line)
		//lines[i] = nline

		//lines[i] = line

		if pos+lLen > c.blockLen {
			pos = 0
		}
		copy(c.lineBlock[pos:], line)
		lines[i] = c.lineBlock[pos : pos+lLen]
		pos += lLen
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
	for lines := range c.lineChan {
		//once.Do(func() {
		//	btime = time.Now()
		//})
		//size += len(lines)
		c.receiver.ConsumeByte(lines)
	}
	c.logger.Info("deal file done ",
		zap.Int("wrong", wrong),
		zap.Int("dealSize", size),
		zap.Duration("cost", time.Since(btime)),
	)
}
