package receiver

import (
	"bytes"
	"fmt"
	"github.com/buaazp/fasthttprouter"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"net/http"
	"os"
	"runtime"
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
	idToTrace            *common.TDataMap

	deleteChan chan string
	finishBool bool
	finishChan chan interface{}
	closeTimes int64
	sync.Mutex

	wrongIdMap  sync.Map
	consumer    *ChannelGroupConsume
	traceNums   int64
	maxSpanNums int
	minSpanNums int
	overWg      sync.WaitGroup
	AutoDetect  bool
	mapMaxSize  int
	mapMinSize  int
	traceMiss   int
	wrongHit    int
}

func (r *Receiver) Run() {
	r.finishBool = false
	//var err error
	r.logger, _ = zap.NewProduction()
	defer r.logger.Sync()
	go func() {
		i := 0
		for {
			if i > 4 {
				r.logger.Info("too long to stop")
				time.Sleep(10 * time.Second)
				os.Exit(0)
			}
			i++
			r.logger.Info("sleep",
				zap.String("port", r.HttpPort),
				zap.Int("i", i),
			)
			time.Sleep(1 * time.Minute)
		}
	}()
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
	go func() {
		i := 0
		for {
			b2Mb := func(b uint64) uint64 {
				return b / 1024 / 1024
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			r.logger.Info("MEM STAT",
				zap.Int("times", i),
				zap.Uint64("Alloc", b2Mb(m.Alloc)),
				zap.Uint64("TotalAlloc", b2Mb(m.TotalAlloc)),
				zap.Uint64("HeapInuse", b2Mb(m.HeapInuse)),
				zap.Uint64("HeapAlloc", b2Mb(m.HeapAlloc)),
				zap.Uint64("Sys", b2Mb(m.Sys)),
				zap.Uint32("NumGC", m.NumGC),
			)
			i++
			time.Sleep(1 * time.Second)
		}
	}()

	r.idToTrace = common.NewTDataMap()

	r.deleteChan = make(chan string, 8000)
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
		r.logger.Info("ReadyHandler", zap.String("port", r.HttpPort))
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
	r.wrongIdMap.Store(id, true)
	//if id == "c074d0a90cd607b" {
	//	r.logger.Info("got wrong example notify",
	//		zap.String("id", id),
	//	)
	//}
	//td := &common.TraceData{
	//	Id:     id,
	//	Source: r.HttpPort,
	//	Md5:    "",
	//	Wrong:  true,
	//	Sbi:    make([]int, 60),
	//}
	//ltd, lexist := r.idToTrace.LoadOrStore(id, td)
	ltd, lexist := r.idToTrace.Load(id)
	if lexist {
		ltd.Wrong = true
		if ltd.Status == common.TraceStatusDone {
			ltd.Status = common.TraceStatusSended
			//b, _ := ltd.Marshal()
			if over == "1" {
				//SendWrongRequestB(ltd, r.CompactorSetWrongUrl, b, "", nil)
				r.SendWrongRequest(id, ltd, r.CompactorSetWrongUrl, "")
			} else {
				//reqPool.Submit(func() {
				//	SendWrongRequest(ltd, r.CompactorSetWrongUrl, over, &r.overWg)
				//})
				go func() {
					//SendWrongRequestB(ltd, r.CompactorSetWrongUrl, b, "", nil)
					r.SendWrongRequest(id, ltd, r.CompactorSetWrongUrl, "")
				}()
			}
		}
	}
	ctx.SetStatusCode(http.StatusOK)
	return
}

func (r *Receiver) ConsumeByte(lines []int) {
	idToSpans := make(map[string]*common.TData, 800)
	for _, val := range lines {
		start := val >> 16
		llen := val & 0xffff
		line := r.consumer.lineBlock[start : start+llen]
		//id, wrong := GetTraceIdWrongByString(line)
		id := GetTraceIdByString(line)
		if etd, ok := idToSpans[id]; !ok {
			td := &common.TData{
				//Source: r.HttpPort,
				Sbi:   make([]int, 0, 50),
				Wrong: IfSpanWrongString(line),
				//Wrong:  wrong,
			}
			//if i > 2_0000 && i < 18_0000 {
			//	td.Status = common.TraceStatusSkip
			//}
			//td.Id = id
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
		td, exist := r.idToTrace.LoadOrStore(id, etd)
		if exist {
			// 已存在
			r.traceMiss++
			td.AddSpani(etd.Sbi)
			if !td.Wrong && etd.Wrong {
				td.Wrong = true
			}
			continue
		}
		//if td.Status == common.TraceStatusSkip {
		//	r.dropTrace(td.Id, td, "0")
		//} else {
		postDeletion := false
		for !postDeletion {
			select {
			case r.deleteChan <- id:
				postDeletion = true
			default:
				//<-r.deleteChan
				dropId, ok := <-r.deleteChan
				if ok {
					r.dropTraceById(dropId, "0")
				}
			}
		}
		//}
	}
}

func (r *Receiver) dropTrace(id string, td *common.TData, over string) {
	atomic.AddInt64(&r.traceNums, 1)
	//td := d.(*common.TraceData)
	//spLen := len(td.Sb)
	//if r.maxSpanNums < spLen {
	//	r.maxSpanNums = spLen
	//}
	//if r.minSpanNums > spLen || r.minSpanNums == 0 {
	//	r.minSpanNums = spLen
	//}
	wrong := td.Wrong
	if !wrong {
		_, ok := r.wrongIdMap.Load(id)
		if ok {
			wrong = true
			r.wrongHit++
		}
	}
	//wrong = false
	if wrong && td.Status != common.TraceStatusSended {
		td.Status = common.TraceStatusSended
		//reqPool.Submit(func() {
		//	SendWrongRequest(td, r.CompactorSetWrongUrl, over, &r.overWg)
		//})
		go r.SendWrongRequest(id, td, r.CompactorSetWrongUrl, over)
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
	r.finishBool = true
	r.overWg.Wait()
	r.logger.Info("finish over",
		zap.Duration("finish cost", ftime),
		zap.Duration("total cost", time.Since(btime)),
		zap.Int64("traceNum", r.traceNums),
		zap.Int("maxSpLen", r.maxSpanNums),
		zap.Int("minSpanNums", r.minSpanNums),
		zap.Int("mapMaxSize", r.mapMaxSize),
		zap.Int("traceMiss", r.traceMiss),
		zap.Int("wrongHit", r.wrongHit),
	)
	r.notifyFIN()
}

func (r *Receiver) notifyFIN() {
	notifyUrl := fmt.Sprintf("http://127.0.0.1:%s/fn?port=%s", r.CompactorPort, r.HttpPort)
	body, err := http.Get(notifyUrl)
	if err != nil {
		r.logger.Info("send notify fin",
			zap.Error(err),
		)
	} else {
		r.logger.Info("send notify fin",
			zap.Int("code", body.StatusCode),
		)
	}

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
	if strings.Contains(l, "error=1") {
		return id, true
	}

	pos := strings.Index(l, "http.status_code=")
	if pos == -1 {
		return id, false
	}
	if l[pos+17:pos+20] != "200" {
		return id, true
	}
	return id, false
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
	l := common.BytesToString(line)
	if strings.Contains(l, "error=1") {
		return true
	}
	pos := strings.Index(l, "http.status_code=")
	if pos == -1 {
		return false
	}
	if l[pos+17:pos+20] != "200" {
		return true
	}
	return false

	//l := common.BytesToString(line)
	//if strings.Contains(l, "error=1") {
	//	return true
	//}
	//pos := strings.Index(l, "http.status_code=")
	//if pos == -1 {
	//	//if strings.Contains(l, "error=1") {
	//	//	return true
	//	//}
	//	return false
	//}
	////return !strings.EqualFold(l[pos+17:pos+20], "200")
	//
	//if l[pos+17:pos+20] != "200" {
	//	return true
	//}
	//return false
}
