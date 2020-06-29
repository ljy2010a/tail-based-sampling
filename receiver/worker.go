package receiver

import (
	"bytes"
	"fmt"
	"go.uber.org/zap"
	"sync"
	"time"
)

func (r *Receiver) Read(dataUrl string) {
	//runtime.LockOSThread()
	//defer runtime.UnlockOSThread()

	logger.Info("read start")
	defer func() {
		err := recover()
		if err != nil {
			logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
		}
	}()
	btime := time.Now()
	size := 0
	total := 0
	//i := 0
	//iLimit := c.linesBatchNum - 1
	//var lines []int
	//select {
	//case lines = <-c.linesCache:
	//default:
	//	lines = make([]int, c.linesBatchNum)
	//}
	//
	//logger.Info("dataUrl", zap.String("dataUrl", dataUrl))
	//resp, err := http.Get(dataUrl)
	//if err != nil {
	//	logger.Error("http get err", zap.Error(err))
	//	return
	//}
	//defer resp.Body.Close()
	//
	//br := NewReaderSize(resp.Body, c.linesBufLen, c.readBufSize, c.linesBuf)
	//for {
	//	start, llen, err := br.ReadSlicePos()
	//	if err != nil {
	//		logger.Info("err", zap.Error(err))
	//		break
	//	}
	//	size += llen
	//	total++
	//
	//	lines[i] = start<<16 | llen
	//
	//	if i == iLimit {
	//		c.linesQueue <- lines
	//		select {
	//		case lines = <-c.linesCache:
	//		default:
	//			lines = make([]int, c.linesBatchNum)
	//		}
	//		i = 0
	//		continue
	//	}
	//	i++
	//}
	//if i != 0 {
	//	c.linesQueue <- lines[:i]
	//}

	hbs := GenRange(dataUrl, linesBufLen)
	downTaskChan := make(chan int, len(hbs))
	for hi, hb := range hbs {
		hb.readBufSize = r.readBufSize
		hb.buf = linesBuf
		downTaskChan <- hi
	}

	for i := 0; i < extDownloader; i++ {
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
		logger.Info("run info",
			zap.Int("seq", hi),
			zap.Int("w", hb.w),
			zap.Int("has w", hb.w-hb.r),
		)
		stime := time.Now()
		if hb.w > hb.r {
			size += hb.w - hb.r
			//total++
			r.readChan <- PP{start: hb.r, llen: hb.w - hb.r}
		}
		for {
			n, err := hb.rd.Read(linesBuf[hb.w:])
			if n > 0 {
				//size += n
				//total++
				r.readChan <- PP{start: hb.w, llen: n}
				hb.w += n
			}
			if err != nil {
				//logger.Info("err", zap.Error(err))
				goto RUN_DOWN
			}
			//if n == 0 {
			//	logger.Info("n = 0",
			//		zap.Int("seq", hi),
			//		zap.Int("hb.w", hb.w),
			//		zap.Int("should end", hb.BufEnd),
			//	)
			//	//goto RUN_DOWN
			//}
		}
	RUN_DOWN:
		logger.Info("run done",
			zap.Int("seq", hi),
			zap.Int("w", hb.w),
			zap.Int("should end", hb.BufEnd),
			zap.Duration("cost", time.Since(stime)),
		)
	}

	rtime := time.Since(btime)
	ctime := time.Now()
	clen := len(r.linesQueue)
	ilen := len(r.readChan)

	logger.Info("load done",
		zap.Duration("total cost", time.Since(btime)),
		zap.Int("linesQueue count", clen),
		zap.Int("readChan count", ilen),
		zap.Int("total", total),
	)

	close(r.readChan)
	//close(c.linesQueue)
	r.doneWg.Wait()
	logger.Info("all done",
		zap.Duration("read cost", rtime),
		zap.Duration("less cost", time.Since(ctime)),
		zap.Duration("total cost", time.Since(btime)),
		zap.Int("linesQueue count", clen),
		zap.Int("readChan count", ilen),
		//zap.Int("maxLine", maxLine),
		//zap.Int("minLine", minLine),
		zap.Int("total", total),
		zap.Int("sourceSize", size),
	)
	close(r.finishSingle)
}

type PP struct {
	start int
	llen  int
}

func (r *Receiver) readIndex() {
	//runtime.LockOSThread()
	//defer runtime.UnlockOSThread()
	defer func() {
		err := recover()
		if err != nil {
			logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
		}
	}()

	r.doneWg.Add(1)
	defer r.doneWg.Done()

	i := 0
	iLimit := linesBatchNum - 1
	var lines []int
	select {
	case lines = <-r.linesCache:
	default:
		lines = make([]int, linesBatchNum)
	}

	readPos := 0
	writePos := 0

	btime := time.Now()
	once := sync.Once{}
	total := 0
	maxLine := 0
	minLine := 500
	for val := range r.readChan {
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
		if writeMaxPos > linesBufLen {
			less := writePos - readPos
			if less > 0 {
				logger.Info("before has less",
					zap.Int("less", less),
				)
				copy(linesBuf[4096-less:4096], linesBuf[readPos:writePos])
				readPos = 4096 - less
				writeMaxPos = start + blen
			}
		}

		//if writeMaxPos == c.linesBufLen {
		//	logger.Info("reach block end", )
		//}

		writePos = writeMaxPos

		for {
			//e := readPos + 512
			//if e >= writePos {
			//	e = writePos
			//}
			if p := bytes.IndexByte(linesBuf[readPos:writePos], '\n'); p >= 0 {
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
				//if total%c.linesBatchNum == 0 {
				//	c.linesQueue <- c.lines[total-c.linesBatchNum : total]
				//}
				if i == iLimit {
					//c.receiver.ConsumeByte(lines)
					r.linesQueue <- lines
					select {
					case lines = <-r.linesCache:
					default:
						lines = make([]int, linesBatchNum)
						logger.Info("need to make lines")
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

	//if total%c.linesBatchNum != 0 {
	//	c.linesQueue <- c.lines[total-total%c.linesBatchNum : total]
	//}

	if i != 0 {
		r.linesQueue <- lines[:i]
	}

	close(r.linesQueue)
	logger.Info("index done",
		zap.Duration("total cost", time.Since(btime)),
		zap.Int("linesQueue less", len(r.linesQueue)),
		zap.Int("maxLine", maxLine),
		zap.Int("minLine", minLine),
		zap.Int("total", total),
	)
}

func (r *Receiver) readLines() {
	//runtime.LockOSThread()
	//defer runtime.UnlockOSThread()
	defer func() {
		err := recover()
		if err != nil {
			logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
		}
	}()
	r.doneWg.Add(1)
	defer r.doneWg.Done()

	btime := time.Now()
	once := sync.Once{}
	//idToSpans := make(map[string]*TData, 100_0000)

	//skipChan := make([]string, 0, 5000)
	//lastChan := make([]string, 0, 5000)
	//tmplastChan := make([]string, 0, 5000)
	//var tdCachePos int64
	//tdCache := make([]*TData, c.receiver.tdCacheLimit)
	//for i := int64(0); i < c.receiver.tdCacheLimit; i++ {
	//	tdCache[i] = NewTData()
	//}
	for lines := range r.linesQueue {
		once.Do(func() {
			btime = time.Now()
		})
		//var idToSpans map[string]*TData
		//select {
		//case idToSpans = <-c.receiver.idMapCache:
		//default:
		//	idToSpans = make(map[string]*TData, 10000)
		//}
		r.ConsumeByte(lines)
		//c.receiver.ConsumeByte(lines, idToSpans)
		//for k := range idToSpans {
		//	delete(idToSpans, k)
		//}

		//for i, val := range lines {
		//	start := val >> 16
		//	llen := val & 0xffff
		//	line := c.linesBuf[start : start+llen]
		//	id := GetTraceIdByString(line)
		//
		//	//etd, eok := c.receiver.idToTrace.Load(id)
		//	//if !eok {
		//	//	c.receiver.tdCachePos += 1
		//	//	if c.receiver.tdCachePos < c.receiver.tdCacheLimit {
		//	//		etd = c.receiver.tdCache[c.receiver.tdCachePos]
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
		//		c.receiver.tdCachePos += 1
		//		if c.receiver.tdCachePos < c.receiver.tdCacheLimit {
		//			etd = c.receiver.tdCache[c.receiver.tdCachePos]
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
		//	if !etd.Wrong && IfSpanWrongString(c.linesBuf[start:start+llen]) {
		//		etd.Wrong = true
		//	}
		//	etd.Sbi = append(etd.Sbi, val)
		//}

		//logger.Info("deal block done ",
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
	logger.Info("deal file done ",
		zap.Duration("cost", time.Since(btime)),
	)
}
