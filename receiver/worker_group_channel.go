package receiver

import (
	"bytes"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strconv"
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
	//lines        []int
	posSlice chan []int
	readChan chan PP
}

func NewChannelGroupConsume(receiver *Receiver, readDone func(), over func()) *ChannelGroupConsume {
	// 500w = 1450MB
	// 1w = 2.6MB
	blockLen := int(2.5 * 1024 * 1024 * 1024)
	readBufSize := 128 * 1024 * 1024
	c := &ChannelGroupConsume{
		receiver:     receiver,
		logger:       receiver.logger,
		lineChan:     make(chan []int, 110),
		lineGroupNum: 250000,
		//lines:        make([]int, 2600_0000),
		readBufSize:  readBufSize,
		workNum:      2,
		readDoneFunc: readDone,
		overFunc:     over,
		blockLen:     blockLen,
		lineBlock:    make([]byte, blockLen),
		posSlice:     make(chan []int, 110),
		readChan:     make(chan PP, 1024),
	}
	for i := 0; i < 110; i++ {
		c.posSlice <- make([]int, c.lineGroupNum)
	}
	return c
}

func (c *ChannelGroupConsume) Read(dataUrl string) {
	//runtime.LockOSThread()
	//defer runtime.UnlockOSThread()

	c.logger.Info("read start")
	defer func() {
		err := recover()
		if err != nil {
			c.logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
		}
	}()
	btime := time.Now()
	size := 0
	total := 0
	//i := 0
	//iLimit := c.lineGroupNum - 1
	//var lines []int
	//select {
	//case lines = <-c.posSlice:
	//default:
	//	lines = make([]int, c.lineGroupNum)
	//}
	//
	//c.logger.Info("dataUrl", zap.String("dataUrl", dataUrl))
	//resp, err := http.Get(dataUrl)
	//if err != nil {
	//	c.logger.Error("http get err", zap.Error(err))
	//	return
	//}
	//defer resp.Body.Close()
	//
	//br := NewReaderSize(resp.Body, c.blockLen, c.readBufSize, c.lineBlock)
	//for {
	//	start, llen, err := br.ReadSlicePos()
	//	if err != nil {
	//		c.logger.Info("err", zap.Error(err))
	//		break
	//	}
	//	size += llen
	//	total++
	//
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
	//if i != 0 {
	//	c.lineChan <- lines[:i]
	//}

	hbs := GenRange(dataUrl, c.blockLen)
	downTaskChan := make(chan int, len(hbs))
	for hi, hb := range hbs {
		hb.readBufSize = c.readBufSize
		hb.buf = c.lineBlock
		downTaskChan <- hi
	}

	for i := 0; i < 1; i++ {
		go func() {
			for hi := range downTaskChan {
				if !hbs[hi].exitRead {
					hbs[hi].wg.Add(1)
					hbs[hi].asyncfill()
				}
			}
		}()
	}
	for hi, hb := range hbs {
		hb.exitRead = true
		close(hb.readSignal)
		hb.wg.Wait()
		c.logger.Info("run info",
			zap.Int("seq", hi),
			zap.Int("w", hb.w),
			zap.Int("has w", hb.w-hb.r),
		)
		stime := time.Now()
		if hb.w > hb.r {
			size += hb.w - hb.r
			total++
			c.readChan <- PP{start: hb.r, llen: hb.w - hb.r}
		}
		for {
			n, err := hb.rd.Read(c.lineBlock[hb.w:])
			if n > 0 {
				size += n
				total++
				c.readChan <- PP{start: hb.w, llen: n}
				hb.w += n
			}
			if err != nil {
				//c.logger.Info("err", zap.Error(err))
				goto RUN_DOWN
			}
			//if n == 0 {
			//	c.logger.Info("n = 0",
			//		zap.Int("seq", hi),
			//		zap.Int("hb.w", hb.w),
			//		zap.Int("should end", hb.BufEnd),
			//	)
			//	//goto RUN_DOWN
			//}
		}
	RUN_DOWN:
		c.logger.Info("run done",
			zap.Int("seq", hi),
			zap.Int("w", hb.w),
			zap.Int("should end", hb.BufEnd),
			zap.Duration("cost", time.Since(stime)),
		)
	}

	rtime := time.Since(btime)
	ctime := time.Now()
	clen := len(c.lineChan)
	ilen := len(c.readChan)

	c.logger.Info("load done",
		zap.Duration("total cost", time.Since(btime)),
		zap.Int("lineChan count", clen),
		zap.Int("readChan count", ilen),
		zap.Int("total", total),
	)

	close(c.readChan)
	//close(c.lineChan)
	c.doneWg.Wait()
	c.logger.Info("all done",
		zap.Duration("read cost", rtime),
		zap.Duration("less cost", time.Since(ctime)),
		zap.Duration("total cost", time.Since(btime)),
		zap.Int("lineChan count", clen),
		zap.Int("readChan count", ilen),
		//zap.Int("maxLine", maxLine),
		//zap.Int("minLine", minLine),
		zap.Int("total", total),
		zap.Int("sourceSize", size),
	)
	c.overFunc()
}

type PP struct {
	start int
	llen  int
}

func (c *ChannelGroupConsume) Index() {
	//runtime.LockOSThread()
	//defer runtime.UnlockOSThread()
	defer func() {
		err := recover()
		if err != nil {
			c.logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
		}
	}()

	c.doneWg.Add(1)
	defer c.doneWg.Done()

	i := 0
	iLimit := c.lineGroupNum - 1
	var lines []int
	select {
	case lines = <-c.posSlice:
	default:
		lines = make([]int, c.lineGroupNum)
	}

	readPos := 0
	writePos := 0

	btime := time.Now()
	once := sync.Once{}
	total := 0
	maxLine := 0
	minLine := 500
	for val := range c.readChan {
		once.Do(func() {
			btime = time.Now()
		})
		start := val.start
		blen := val.llen
		//logger.Info("read load ",
		//	zap.Int("start", start),
		//	zap.Int("blen", blen),
		//)
		writeMaxPos := writePos + blen
		if writeMaxPos > c.blockLen {
			less := writePos - readPos
			if less > 0 {
				logger.Info("before has less",
					zap.Int("less", less),
				)
				copy(c.lineBlock[20*1024*1024-less:20*1024*1024], c.lineBlock[readPos:writePos])
				readPos = 20*1024*1024 - less
				writeMaxPos = start + blen
			}
		}

		//if writeMaxPos == c.blockLen {
		//	logger.Info("reach block end", )
		//}

		writePos = writeMaxPos

		for {
			//s := 128
			//if readPos+s > writePos {
			//	s = 0
			//}
			if p := bytes.IndexByte(c.lineBlock[readPos:writePos], '\n'); p >= 0 {
				//logger.Info("readPos",
				//	zap.Int("readPos", readPos),
				//	zap.Int("blen", blen),
				//)
				//l := readPos<<16 | p + 1
				//p = p + s
				//c.lines[total] = readPos<<16 | p + 1
				lines[i] = readPos<<16 | p + 1
				//lines = append(lines, l)

				//if p > maxLine {
				//	maxLine = p
				//}
				//
				//if p < minLine {
				//	minLine = p
				//}

				readPos += p + 1
				//total++
				//if total%c.lineGroupNum == 0 {
				//	c.lineChan <- c.lines[total-c.lineGroupNum : total]
				//}
				if i == iLimit {
					//c.receiver.ConsumeByte(lines)
					c.lineChan <- lines
					select {
					case lines = <-c.posSlice:
					default:
						lines = make([]int, c.lineGroupNum)
						c.logger.Info("need to make lines")
					}
					i = 0
					continue
				}
				i++
				continue
			} else {
				break
			}
		}

	}

	//if total%c.lineGroupNum != 0 {
	//	c.lineChan <- c.lines[total-total%c.lineGroupNum : total]
	//}

	if i != 0 {
		c.lineChan <- lines[:i]
	}

	close(c.lineChan)
	c.logger.Info("index done",
		zap.Duration("total cost", time.Since(btime)),
		zap.Int("lineChan less", len(c.lineChan)),
		zap.Int("maxLine", maxLine),
		zap.Int("minLine", minLine),
		zap.Int("total", total),
	)
}

func (c *ChannelGroupConsume) StartConsume() {
	for i := 0; i < c.workNum; i++ {
		go c.consume()
	}
	go c.Index()
}

func (c *ChannelGroupConsume) consume() {
	//runtime.LockOSThread()
	//defer runtime.UnlockOSThread()
	defer func() {
		err := recover()
		if err != nil {
			c.logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
		}
	}()
	c.doneWg.Add(1)
	defer c.doneWg.Done()

	btime := time.Now()
	once := sync.Once{}
	//idToSpans := make(map[string]*TData, 100_0000)

	//skipChan := make([]string, 0, 5000)
	//lastChan := make([]string, 0, 5000)
	//tmplastChan := make([]string, 0, 5000)
	//var tdSlicePos int64
	//tdSlice := make([]*TData, c.receiver.tdSliceLimit)
	//for i := int64(0); i < c.receiver.tdSliceLimit; i++ {
	//	tdSlice[i] = NewTData()
	//}
	for lines := range c.lineChan {
		once.Do(func() {
			btime = time.Now()
		})
		//var idToSpans map[string]*TData
		//select {
		//case idToSpans = <-c.receiver.idPool:
		//default:
		//	idToSpans = make(map[string]*TData, 10000)
		//}
		c.receiver.ConsumeByte(lines)
		//c.receiver.ConsumeByte(lines, idToSpans)
		//for k := range idToSpans {
		//	delete(idToSpans, k)
		//}

		//for i, val := range lines {
		//	start := val >> 16
		//	llen := val & 0xffff
		//	line := c.lineBlock[start : start+llen]
		//	id := GetTraceIdByString(line)
		//
		//	//etd, eok := c.receiver.idToTrace.Load(id)
		//	//if !eok {
		//	//	c.receiver.tdSlicePos += 1
		//	//	if c.receiver.tdSlicePos < c.receiver.tdSliceLimit {
		//	//		etd = c.receiver.tdSlice[c.receiver.tdSlicePos]
		//	//	} else {
		//	//		etd = NewTData()
		//	//	}
		//	//	etd, eok = c.receiver.idToTrace.LoadOrStore(id, etd)
		//	//}
		//	//if !etd.Wrong && IfSpanWrongString(line) {
		//	//	etd.Wrong = true
		//	//}
		//	//etd.Sbi = append(etd.Sbi, val)
		//
		//	var etd *TData
		//	var ok bool
		//	if etd, ok = idToSpans[id]; !ok {
		//		c.receiver.tdSlicePos += 1
		//		if c.receiver.tdSlicePos < c.receiver.tdSliceLimit {
		//			etd = c.receiver.tdSlice[c.receiver.tdSlicePos]
		//		} else {
		//			etd = NewTData()
		//		}
		//		idToSpans[id] = etd
		//		if i > 2_0000 && i < 8_0000 {
		//			etd.Status = common.TraceStatusSkip
		//			//	skipChan = append(skipChan, id)
		//		} else {
		//			//	tmplastChan = append(tmplastChan, id)
		//		}
		//	}
		//
		//	if !etd.Wrong && IfSpanWrongString(c.lineBlock[start:start+llen]) {
		//		etd.Wrong = true
		//	}
		//	etd.Sbi = append(etd.Sbi, val)
		//}

		//c.logger.Info("deal block done ",
		//	zap.Int("lastChan", len(lastChan)),
		//	zap.Int("tmplastChan", len(tmplastChan)),
		//	zap.Int("skipChan", len(skipChan)),
		//)
		//for z := range lastChan {
		//	c.receiver.dropTrace(lastChan[z], idToSpans[lastChan[z]], "")
		//}
		//lastChan = lastChan[:0]
		//lastChan, tmplastChan = tmplastChan, lastChan
		//for z := range skipChan {
		//	c.receiver.dropTrace(skipChan[z], idToSpans[skipChan[z]], "")
		//}
		//skipChan = skipChan[:0]
	}
	c.logger.Info("deal file done ",
		zap.Duration("cost", time.Since(btime)),
	)
}

type HttpBlock struct {
	Seq       int
	HttpStart int
	HttpEnd   int
	BufStart  int
	BufEnd    int

	rd                io.Reader
	readSignal        chan interface{}
	exitRead          bool
	buf               []byte
	r, w, readBufSize int
	err               error
	wg                sync.WaitGroup
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

		tmpStepSize := stepSize
		if bufEnd == bufSize {
			bufStart = 20 * 1024 * 1024
			bufEnd = bufStart + tmpStepSize
		} else {
			if bufStart+tmpStepSize > bufSize {
				tmpStepSize = bufSize - bufStart
				bufEnd = bufSize
			} else {
				bufEnd = bufStart + tmpStepSize
			}
		}

		httpEnd = httpStart + tmpStepSize - 1
		if httpEnd >= length {
			httpEnd = length
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
			rd:         rd,
			r:          bufStart,
			w:          bufStart,
			readSignal: make(chan interface{}),
		})
		bufStart = bufEnd
		httpStart = httpEnd + 1
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
