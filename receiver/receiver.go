package receiver

import (
	"bytes"
	"fmt"
	"github.com/buaazp/fasthttprouter"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Receiver struct {
	HttpPort             string // 8000,8001
	DataPort             string // 8081
	CompactorPort        string // 8002
	CompactorSetWrongUrl string
	logger               *zap.Logger
	idToTrace            *TDataMap

	deleteChan chan string
	finishChan chan interface{}
	closeTimes int64
	sync.Mutex

	//wrongIdMap  sync.Map
	consumer    *ChannelGroupConsume
	traceNums   int64
	maxSpanNums int
	minSpanNums int
	overWg      sync.WaitGroup
	AutoDetect  bool
	mapMaxSize  int
	mapMinSize  int
	traceMiss   int
	traceSkip   int
	wrongHit    int
	tdSlice     chan *TData
}

func (r *Receiver) Run() {
	//var err error
	r.logger, _ = zap.NewProduction()
	defer r.logger.Sync()
	//go func() {
	//	i := 0
	//	for {
	//		if i > 4 {
	//			r.logger.Info("too long to stop")
	//			time.Sleep(10 * time.Second)
	//			os.Exit(0)
	//		}
	//		i++
	//		r.logger.Info("sleep",
	//			zap.String("port", r.HttpPort),
	//			zap.Int("i", i),
	//		)
	//		time.Sleep(1 * time.Minute)
	//	}
	//}()
	go func() {
		if !r.AutoDetect {
			return
		}
		time.Sleep(5 * time.Second)
		if r.DataPort != "" {
			r.logger.Info("has dataport")
			return
		}
		r.logger.Info("try to detect")

		port := 8000
		for i := 0; i < 2000; i++ {
			port++
			dataUrl := fmt.Sprintf("http://127.0.0.1:%d/trace1.data", port)
			resp, err := http.Get(dataUrl)
			if err != nil {
				continue
			}
			resp.Body.Close()
			r.logger.Info("detect port",
				zap.Int("port", port),
				zap.Int("code", resp.StatusCode),
			)

			if resp.StatusCode == 200 && r.HttpPort == "8000" {
				r.DataPort = fmt.Sprintf("%d", port)

				dataUrl := fmt.Sprintf("http://127.0.0.1:%s/trace1.data", r.DataPort)
				go r.ReadHttp(dataUrl)
				return
			}

			if resp.StatusCode == 200 && r.HttpPort == "8001" {
				r.DataPort = fmt.Sprintf("%d", port)
				dataUrl2 := fmt.Sprintf("http://127.0.0.1:%s/trace2.data", r.DataPort)
				go r.ReadHttp(dataUrl2)
				return
			}
		}
	}()
	//go func() {
	//	i := 0
	//	for {
	//		b2Mb := func(b uint64) uint64 {
	//			return b / 1024 / 1024
	//		}
	//		var m runtime.MemStats
	//		runtime.ReadMemStats(&m)
	//
	//		r.logger.Info("MEM STAT",
	//			zap.Int("times", i),
	//			zap.Uint64("Alloc", b2Mb(m.Alloc)),
	//			zap.Uint64("TotalAlloc", b2Mb(m.TotalAlloc)),
	//			zap.Uint64("HeapInuse", b2Mb(m.HeapInuse)),
	//			zap.Uint64("HeapAlloc", b2Mb(m.HeapAlloc)),
	//			zap.Uint64("Sys", b2Mb(m.Sys)),
	//			zap.Uint32("NumGC", m.NumGC),
	//		)
	//		i++
	//		time.Sleep(1 * time.Second)
	//	}
	//}()

	tdNums := 120_0000
	tdSlice := make(chan *TData, tdNums)
	for i := 0; i < tdNums; i++ {
		tdSlice <- NewTData()
	}
	r.idToTrace = NewTDataMap()

	r.deleteChan = make(chan string, 6000)
	r.finishChan = make(chan interface{})
	doneFunc := func() {
	}
	overFunc := func() {
		close(r.finishChan)
	}
	r.consumer = NewChannelGroupConsume(r, doneFunc, overFunc)
	r.consumer.StartConsume()

	go r.finish()

	frouter := fasthttprouter.New()
	frouter.GET("/ready", func(ctx *fasthttp.RequestCtx) {
		btime := time.Now()
		wg := sync.WaitGroup{}
		for i := 0; i < 500; i++ {
			go r.warmUp(wg)
		}
		wg.Wait()
		r.logger.Info("ReadyHandler done",
			zap.String("port", r.HttpPort),
			zap.Duration("cost", time.Since(btime)))
		ctx.SetStatusCode(http.StatusOK)
	})
	frouter.GET("/warmup", func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(http.StatusOK)
	})
	frouter.GET("/setParameter", r.SetParamHandler)
	frouter.GET("/qw", r.QueryWrongHandler)
	if err := fasthttp.ListenAndServe(fmt.Sprintf(":%s", r.HttpPort), frouter.Handler); err != nil {
		r.logger.Info("r.HttpPort fail", zap.Error(err))
	}
}

func (r *Receiver) SetParamHandler(ctx *fasthttp.RequestCtx) {
	port := string(ctx.QueryArgs().Peek("port"))
	r.DataPort = port
	r.logger.Info("SetParamHandler",
		zap.String("port", r.HttpPort),
		zap.String("set", r.DataPort),
	)

	if r.HttpPort == "8000" {
		dataUrl := fmt.Sprintf("http://127.0.0.1:%s/trace1.data", r.DataPort)
		go r.ReadHttp(dataUrl)
	}

	if r.HttpPort == "8001" {
		dataUrl2 := fmt.Sprintf("http://127.0.0.1:%s/trace2.data", r.DataPort)
		go r.ReadHttp(dataUrl2)
	}
	ctx.SetStatusCode(http.StatusOK)
	return
}

func (r *Receiver) QueryWrongHandler(ctx *fasthttp.RequestCtx) {
	id := string(ctx.QueryArgs().Peek("id"))
	over := string(ctx.QueryArgs().Peek("over"))
	//r.wrongIdMap.Store(id, true)
	//if id == "c074d0a90cd607b" {
	//	r.logger.Info("got wrong example notify",
	//		zap.String("id", id),
	//	)
	//}
	//td := &common.TData{
	//	Status: common.TraceStatusWrongSet,
	//	Wrong:  true,
	//	Sbi:    make([]int, 0, 50),
	//}
	var td *TData
	select {
	case td = <-r.tdSlice:
	default:
		td = NewTData()
	}
	td.Wrong = true
	td.Status = common.TraceStatusWrongSet
	ltd, lexist := r.idToTrace.LoadOrStore(id, td)
	if lexist {
		ltd.Wrong = true
		if ltd.Status == common.TraceStatusDone {
			ltd.Status = common.TraceStatusSended
			if over == "1" {
				r.SendWrongRequest(id, ltd, "")
			} else {
				//reqPool.Submit(func() {
				//	r.SendWrongRequest(id, ltd, r.CompactorSetWrongUrl, "")
				//})
				go func() {
					r.SendWrongRequest(id, ltd, "")
				}()
			}
		}
	}
	ctx.SetStatusCode(http.StatusOK)
	return
}

func (r *Receiver) ConsumeByte(lines []int, idToSpans map[string]*TData) {
	for i, val := range lines {
		start := val >> 16
		llen := val & 0xffff
		line := r.consumer.lineBlock[start : start+llen]
		//id, wrong := GetTraceIdWrongByString(line)
		id := GetTraceIdByString(line)
		if etd, ok := idToSpans[id]; !ok {
			var td *TData
			select {
			case td = <-r.tdSlice:
			default:
				td = NewTData()
			}
			td.Wrong = IfSpanWrongString(line)
			//td.Wrong = wrong
			if i > 2_0000 && i < 23_0000 {
				td.Status = common.TraceStatusSkip
			}
			td.Sbi = append(td.Sbi, val)
			idToSpans[id] = td
		} else {
			if !etd.Wrong && IfSpanWrongString(line) {
				etd.Wrong = true
			}
			//if !etd.Wrong && wrong {
			//	etd.Wrong = true
			//}
			etd.Sbi = append(etd.Sbi, val)
		}
	}

	//mapSize := len(idToSpans)
	//if mapSize > r.mapMaxSize {
	//	r.mapMaxSize = mapSize
	//}

	for id, etd := range idToSpans {
		if etd.Status == common.TraceStatusSkip && etd.Wrong {
			r.traceSkip++
			r.dropTrace(id, etd, "0")
			continue
		}
		td, exist := r.idToTrace.LoadOrStore(id, etd)
		if exist {
			// 已存在
			r.traceMiss++
			//td.AddSpani(etd.Sbi)
			td.Sbi = append(td.Sbi, etd.Sbi...)
			if !td.Wrong && etd.Wrong {
				td.Wrong = true
			}
			if td.Status == common.TraceStatusWrongSet {
				goto SET_AND_DROP
			}
			continue
		}
		if td.Status == common.TraceStatusSkip {
			r.traceSkip++
			r.dropTrace(id, td, "0")
			continue
		}
	SET_AND_DROP:
		postDeletion := false
		for !postDeletion {
			select {
			case r.deleteChan <- id:
				postDeletion = true
			default:
				dropId, ok := <-r.deleteChan
				if ok {
					r.dropTraceById(dropId, "0")
				}
			}
		}
	}
}

func (r *Receiver) dropTrace(id string, td *TData, over string) {
	atomic.AddInt64(&r.traceNums, 1)
	//spLen := len(td.Sb)
	//if r.maxSpanNums < spLen {
	//	r.maxSpanNums = spLen
	//}
	//if r.minSpanNums > spLen || r.minSpanNums == 0 {
	//	r.minSpanNums = spLen
	//}
	wrong := td.Wrong
	//if !wrong {
	//	_, ok := r.wrongIdMap.Load(id)
	//	if ok {
	//		wrong = true
	//		r.wrongHit++
	//	}
	//}
	//wrong = false
	if wrong && td.Status != common.TraceStatusSended {
		td.Status = common.TraceStatusSended
		//reqPool.Submit(func() {
		//	r.SendWrongRequest(id, td, r.CompactorSetWrongUrl, over)
		//})
		go r.SendWrongRequest(id, td, over)
		return
	} else {
		td.Status = common.TraceStatusDone
	}
}

func (r *Receiver) dropTraceById(id string, over string) {
	td, ok := r.idToTrace.Load(id)
	if !ok {
		r.logger.Info("drop id not exist", zap.String("id", id))
		return
	}
	r.dropTrace(id, td, over)
}

func (r *Receiver) finish() {
	btime := time.Now()
	ftime := time.Second
	fwg := sync.WaitGroup{}
	fonce := sync.Once{}
	for i := 0; i < 2; i++ {
		fwg.Add(1)
		go func() {
			defer fwg.Done()
			<-r.finishChan
			fonce.Do(func() {
				r.logger.Info("finish start")
				btime = time.Now()
			})
			for {
				select {
				case dropId := <-r.deleteChan:
					r.dropTraceById(dropId, "1")
				default:
					ftime = time.Since(btime)
					return
				}
			}

		}()
	}
	fwg.Wait()
	r.overWg.Wait()
	r.logger.Info("finish over",
		zap.Duration("finish cost", ftime),
		zap.Duration("total cost", time.Since(btime)),
		zap.Int64("traceNum", r.traceNums),
		zap.Int("maxSpLen", r.maxSpanNums),
		zap.Int("minSpanNums", r.minSpanNums),
		zap.Int("mapMaxSize", r.mapMaxSize),
		zap.Int("traceMiss", r.traceMiss),
		zap.Int("traceSkip", r.traceSkip),
		zap.Int("wrongHit", r.wrongHit),
	)
	r.notifyFIN()
}

var (
	FCode    = []byte("http.status_code=")
	FCode200 = []byte("http.status_code=200")
	Ferr1    = []byte("error=1")
)

func GetTraceIdWrongByString(line []byte) (string, bool) {
	//firstIdx := bytes.IndexByte(line, '|')
	//id := common.BytesToString(line[:firstIdx])
	l := common.BytesToString(line)
	id := l[:strings.IndexByte(l, '|')]
	llen := len(l)
	pos := strings.Index(l, "http.status_code=")
	if pos == -1 {
		//if strings.Contains(l, "error=1") {
		//	return id, true
		//}
		if l[llen-7] != 'e' {
			return id, false
		}
		if l[llen-6] != 'r' {
			return id, false
		}
		if l[llen-5] != 'r' {
			return id, false
		}
		if l[llen-4] != 'o' {
			return id, false
		}
		if l[llen-3] != 'r' {
			return id, false
		}
		if l[llen-2] != '=' {
			return id, false
		}
		if l[llen-1] != '1' {
			return id, false
		}
		return id, true
	}

	if l[pos+17] != '2' {
		return id, true
	}
	if l[pos+18] != '0' {
		return id, true
	}
	if l[pos+19] != '0' {
		return id, true
	}
	return id, false

	//return id, !strings.EqualFold(l[pos+17:pos+20], "200")
	//if strings.Contains(l, "error=1") {
	//	return id, true
	//}
	//
	//pos := strings.Index(l, "http.status_code=")
	//if pos == -1 {
	//	return id, false
	//}
	//if l[pos+17:pos+20] != "200" {
	//	return id, true
	//}
	//return id, false
}

func GetTraceIdWrongByByte(line []byte) (string, bool) {
	firstIdx := bytes.IndexByte(line, '|')
	id := common.BytesToString(line[:firstIdx])
	if bytes.Contains(line, Ferr1) {
		return id, true
	}
	if bytes.Contains(line, FCode) && !bytes.Contains(line, FCode200) {
		return id, true
	}
	return id, false
}

func GetTraceIdByString(line []byte) string {
	l := common.BytesToString(line)
	return l[:strings.IndexByte(l, '|')]
}

func GetTraceIdByByte(line []byte) string {
	firstIdx := bytes.IndexByte(line, '|')
	return common.BytesToString(line[:firstIdx])
}

func IfSpanWrongByte(line []byte) bool {
	if bytes.Contains(line, Ferr1) {
		return true
	}
	if bytes.Contains(line, FCode) && !bytes.Contains(line, FCode200) {
		return true
	}
	return false
}

func IfSpanWrongString(line []byte) bool {
	//l := common.BytesToString(line)
	//if strings.Contains(l, "error=1") {
	//	return true
	//}
	//pos := strings.Index(l, "http.status_code=")
	//if pos == -1 {
	//	return false
	//}
	//if l[pos+17:pos+20] != "200" {
	//	return true
	//}
	//return false

	l := common.BytesToString(line)
	llen := len(l)
	//if strings.Contains(l, "error=1") {
	//	return true
	//}
	pos := strings.Index(l, "http.status_code=")
	if pos == -1 {
		//if strings.Contains(l, "error=1") {
		//	return true
		//}
		if l[llen-7] != 'e' {
			return false
		}
		if l[llen-6] != 'r' {
			return false
		}
		if l[llen-5] != 'r' {
			return false
		}
		if l[llen-4] != 'o' {
			return false
		}
		if l[llen-3] != 'r' {
			return false
		}
		if l[llen-2] != '=' {
			return false
		}
		if l[llen-1] != '1' {
			return false
		}
		return true
		//return false
	}
	if l[pos+17] != '2' {
		return true
	}
	if l[pos+18] != '0' {
		return true
	}
	if l[pos+19] != '0' {
		return true
	}
	return false
	//if l[pos+17:pos+20] != "200" {
	//	return true
	//}
	//return false
}
