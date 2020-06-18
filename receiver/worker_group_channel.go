package receiver

import (
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strconv"
	"strings"
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
	// 1w = 2.6MB
	blockLen := int(2*1024*1024*1024) + 4096*50
	readBufSize := 128 * 1024 * 1024
	c := &ChannelGroupConsume{
		receiver:     receiver,
		logger:       receiver.logger,
		lineChan:     make(chan []int, 80),
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

func (c *ChannelGroupConsume) Read(dataUrl string) {
	//runtime.LockOSThread()
	//defer runtime.UnlockOSThread()

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
	var lines []int
	select {
	case lines = <-c.posSlice:
	default:
		lines = make([]int, c.lineGroupNum)
	}

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

	//br := NewReaderSize(rd, c.blockLen, c.readBufSize, c.lineBlock)
	////br := bufio.NewReaderSize(rd, c.readBufSize)
	//for {
	//	//line, err := br.ReadSlice('\n')
	//	start, llen, err := br.ReadSlicePos('\n')
	//	if err != nil {
	//		c.logger.Info("err", zap.Error(err))
	//		break
	//	}
	//	size += llen
	//	total++
	//
	//	//lines[i] = line
	//	lines[i] = start<<16 | llen
	//
	//	if i == iLimit {
	//		c.lineChan <- lines
	//		select {
	//		case lines = <-c.posSlice:
	//		default:
	//			lines = make([]int, c.lineGroupNum)
	//		}
	//		i = 0
	//		continue
	//	}
	//	i++
	//}

	hbs := GenRange(dataUrl, c.blockLen)
	downTaskChan := make(chan int, len(hbs))
	for hi, hb := range hbs {
		br := &HReader{}
		br.readBufSize = c.readBufSize
		br.bufSize = c.blockLen
		br.buf = c.lineBlock
		br.rd = hb.ioReader

		br.w = hb.BufStart
		br.r = hb.BufStart
		hb.bufReader = br
		downTaskChan <- hi
	}

	//for i := 0; i < 2; i++ {
	//	go func() {
	//		for hi := range downTaskChan {
	//			if !hbs[hi].exitRead {
	//				logger.Info("rush", zap.Int("seq", hbs[hi].Seq))
	//				hbs[hi].wg.Add(1)
	//				hbs[hi].bufReader.asyncfill(hbs[hi].readSignal, &hbs[hi].wg, hbs[hi].Seq)
	//			}
	//		}
	//	}()
	//}

	//idToSpans := make(map[string]*TData, 1024)

	//logger.Info("rush", zap.Int("seq", 0))
	//hbs[0].wg.Add(1)
	//go hbs[0].bufReader.asyncfill(hbs[0].readSignal, &hbs[0].wg, 0)
	//
	//hbs[1].wg.Add(1)
	//go hbs[1].bufReader.asyncfill(hbs[1].readSignal, &hbs[1].wg, 1)

	for hi, hb := range hbs {
		//nextHi := hi + 2
		//if nextHi < len(hbs) {
		//	logger.Info("rush", zap.Int("seq", nextHi))
		//	hbs[nextHi].wg.Add(1)
		//	go hbs[nextHi].bufReader.asyncfill(hbs[nextHi].readSignal, &hbs[nextHi].wg, nextHi)
		//}
		// check last less
		beforeHi := hi - 1
		if beforeHi >= 0 {
			less := hbs[beforeHi].bufReader.w - hbs[beforeHi].bufReader.r
			if less > 0 {
				copy(c.lineBlock[hb.BufStart-less:hb.BufStart], c.lineBlock[hbs[beforeHi].bufReader.r:hbs[beforeHi].bufReader.w])
				hb.bufReader.r = hb.BufStart - less
				//logger.Info("before has less",
				//	zap.String("s", string(c.lineBlock[hb.BufStart-less:hb.BufStart])),
				//)
			}
		}
		c.logger.Info("run info", zap.Int("seq", hi), zap.Int("has w", hb.bufReader.w-hb.bufReader.r))
		//hb.exitRead = true
		//close(hb.readSignal)
		//hb.wg.Wait()
		stime := time.Now()
		for {
			start, llen, err := hb.bufReader.ReadSlicePos()
			if err != nil {
				//c.logger.Info("err", zap.Int("seq", hi), zap.Error(err))
				break
			}
			size += llen
			total++
			//if llen < 150 {
			//	c.logger.Info("wrong llen", zap.Int("seq", hi), zap.Int("llen", llen))
			//	continue
			//}
			//if llen > 1000 {
			//	c.logger.Info("wrong llen", zap.Int("seq", hi), zap.Int("llen", llen))
			//}
			//c.logger.Info("p", zap.Int("start", start), zap.Int("llen", llen), zap.Int("total", total))
			//c.logger.Info("p", zap.String("s", string(c.lineBlock[start:start+llen])), zap.Int("total", total))
			lines[i] = start<<16 | llen

			//GetTraceIdByString2(c.lineBlock[start:start+llen], total, llen)
			//IfSpanWrongString(c.lineBlock[start : start+llen])

			if i == iLimit {
				c.lineChan <- lines
				//c.receiver.ConsumeByte(lines, idToSpans)
				//for k := range idToSpans {
				//	delete(idToSpans, k)
				//}
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
		c.logger.Info("run done",
			zap.Int("seq", hi),
			zap.Duration("cost", time.Since(stime)),
			zap.Int("less count", len(c.lineChan)),
		)
	}

	if i != 0 {
		c.lineChan <- lines[:i]
	}
	rtime := time.Since(btime)
	ctime := time.Now()
	clen := len(c.lineChan)
	//c.readDoneFunc()
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

func GetTraceIdByString2(line []byte, total, llen int) string {
	defer func() {
		err := recover()
		if err != nil {
			logger.Info("",
				zap.String("l", string(line)),
				zap.Int("total", total),
				zap.Int("len", llen),
			)
			time.Sleep(5 * time.Second)
			panic(err)
		}
	}()
	l := common.BytesToString(line)
	return l[:strings.IndexByte(l, '|')]
}

func (c *ChannelGroupConsume) StartConsume() {
	for i := 0; i < c.workNum; i++ {
		go c.consume()
	}
}

func (c *ChannelGroupConsume) consume() {
	//runtime.LockOSThread()
	//defer runtime.UnlockOSThread()
	//defer func() {
	//	err := recover()
	//	if err != nil {
	//		c.logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
	//	}
	//}()
	c.doneWg.Add(1)
	defer c.doneWg.Done()

	btime := time.Now()
	once := sync.Once{}
	idToSpans := make(map[string]*TData, 1024)
	for lines := range c.lineChan {
		once.Do(func() {
			btime = time.Now()
		})
		c.receiver.ConsumeByte(lines, idToSpans)
		for k := range idToSpans {
			delete(idToSpans, k)
		}
	}
	c.logger.Info("deal file done ",
		zap.Duration("cost", time.Since(btime)),
	)
}

type HttpBlock struct {
	Seq        int
	HttpStart  int
	HttpEnd    int
	BufStart   int
	BufEnd     int
	ioReader   io.Reader
	bufReader  *HReader
	readSignal chan interface{}
	exitRead   bool
	wg         sync.WaitGroup
}

func GenRange(dataUrl string, bufSize int) []*HttpBlock {
	btime := time.Now()
	logger.Info("dataUrl", zap.String("dataUrl", dataUrl))
	client := &http.Client{}
	req, err := http.NewRequest("HEAD", dataUrl, nil)
	if err != nil {
		logger.Error("http get err", zap.Error(err))
		return nil
	}
	req.Header.Add("Range", "bytes=0-")
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("http get err", zap.Error(err))
		return nil
	}

	var length int
	switch resp.StatusCode {
	case http.StatusPartialContent:
		contentLength := resp.Header.Get("Content-Length")
		length64, _ := strconv.ParseInt(contentLength, 10, 0)
		length = int(length64)
	default:
		length = 0
	}

	logger.Info("http file info",
		zap.String("fileUrl", dataUrl),
		zap.Duration("head cost", time.Since(btime)),
		zap.Int("len", length),
	)

	stepSize := 512 * 1024 * 1024
	hbs := make([]*HttpBlock, 0, length/stepSize+1)
	bufStart := 0
	bufEnd := 0
	httpStart := 0
	httpEnd := 0
	seq := 0
	for {
		if httpEnd == length {
			break
		}

		httpEnd = httpStart + stepSize
		if httpEnd >= length {
			httpEnd = length
		}
		bufEnd = bufStart + stepSize
		if bufEnd > bufSize {
			bufStart = 1024
			bufEnd = bufStart + stepSize
		}
		rd, err := httpGet(dataUrl, httpStart, httpEnd)
		if err != nil {
			logger.Error("http get err",
				zap.Error(err),
			)
			return nil
		}
		logger.Info("hb",
			zap.Int("seq", seq),
			zap.Int("httpStart", httpStart),
			zap.Int("httpEnd", httpEnd),
			zap.Int("bufStart", bufStart),
			zap.Int("bufEnd", bufEnd),
		)
		hbs = append(hbs, &HttpBlock{
			Seq:        seq,
			HttpStart:  httpStart,
			HttpEnd:    httpEnd,
			BufStart:   bufStart,
			BufEnd:     bufEnd,
			ioReader:   rd,
			readSignal: make(chan interface{}),
			//exitRead:   make(chan interface{}),
		})
		bufStart = bufEnd + 1024
		httpStart = httpEnd
		seq++
	}
	return hbs
}

func httpGet(url string, start, end int) (io.Reader, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}
