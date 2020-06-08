package receiver

import (
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"go.uber.org/zap"
	"net/http"
	"os"
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
	idToTrace            sync.Map

	deleteChan chan string
	finishBool bool
	finishChan chan interface{}
	closeTimes int64
	sync.Mutex

	wrongIdMap sync.Map
	lruCache   *lru.Cache
	//consumer   *ChannelConsume
	consumer    *ChannelGroupConsume
	traceNums   int64
	maxSpanNums int
	minSpanNums int
	overWg      sync.WaitGroup
	//tdPool      *sync.Pool
	//spanPool    *sync.Pool
	AutoDetect bool
}

func (r *Receiver) Run() {
	r.finishBool = false
	var err error
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
		time.Sleep(30 * time.Second)
		if r.DataPort != "" {
			r.logger.Info("has dataport")
			return
		}
		r.logger.Info("try to detect")

		port := 8000
		for i := 0; i < 1000; i++ {
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
	//	for {
	//		time.Sleep(2 * time.Second)
	//		b2Mb := func(b uint64) uint64 {
	//			return b / 1024 / 1024
	//		}
	//		var m runtime.MemStats
	//		runtime.ReadMemStats(&m)
	//
	//		r.logger.Info("MEM STAT",
	//			zap.Uint64("Alloc", b2Mb(m.Alloc)),
	//			zap.Uint64("TotalAlloc", b2Mb(m.TotalAlloc)),
	//			zap.Uint64("HeapInuse", b2Mb(m.HeapInuse)),
	//			zap.Uint64("HeapAlloc", b2Mb(m.HeapAlloc)),
	//			zap.Uint64("Sys", b2Mb(m.Sys)),
	//			zap.Uint32("NumGC", m.NumGC),
	//		)
	//	}
	//}()
	//r.tdPool = &sync.Pool{
	//	New: func() interface{} {
	//		return &common.TraceData{
	//			//Sd:     []*common.SpanData{},
	//			Source: r.HttpPort,
	//		}
	//	},
	//}
	//
	//r.spanPool = &sync.Pool{
	//	New: func() interface{} {
	//		return &common.SpanData{}
	//	},
	//}

	// 10000条 = 2.9MB
	// 6.5 * 20 * 2.9 = 377
	//r.lruCache, err = lru.NewWithEvict(7_0000, func(key interface{}, value interface{}) {
	//	td := value.(*common.TraceData)
	//	for i, _ := range td.Sd {
	//		td.Sd[i].Reset()
	//		r.spanPool.Put(td.Sd[i])
	//	}
	//	td.Clear()
	//	r.tdPool.Put(td)
	//})
	r.lruCache, err = lru.New(5_0000)
	if err != nil {
		r.logger.Error("lru new fail",
			zap.Error(err),
		)
	}

	r.deleteChan = make(chan string, 3000)
	r.finishChan = make(chan interface{})
	doneFunc := func() {
		r.lruCache.Resize(15_0000)
	}
	overFunc := func() {
		close(r.finishChan)
	}
	//r.consumer = NewChannelConsume(r, overFunc)
	r.consumer = NewChannelGroupConsume(r, doneFunc, overFunc)
	r.consumer.StartConsume()

	go r.finish()

	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/ready", r.ReadyHandler)
	router.GET("/setParameter", r.SetParamHandler)
	router.GET("/qw", r.QueryWrongHandler)
	err = router.Run(fmt.Sprintf(":%s", r.HttpPort))
	if err != nil {
		r.logger.Info("r.HttpPort fail", zap.Error(err))
	}
}

func (r *Receiver) ReadyHandler(c *gin.Context) {
	r.logger.Info("ReadyHandler", zap.String("port", r.HttpPort))
	c.JSON(http.StatusOK, "ok")
	return
}

func (r *Receiver) SetParamHandler(c *gin.Context) {
	port := c.DefaultQuery("port", "")
	r.DataPort = port
	r.logger.Info("SetParamHandler",
		zap.String("port", r.HttpPort),
		zap.String("set", r.DataPort),
	)

	if r.HttpPort == "8000" {
		dataUrl := fmt.Sprintf("http://127.0.0.1:%s/trace1.data", r.DataPort)
		go r.ReadHttp(dataUrl)

		//dataUrl2 := fmt.Sprintf("http://127.0.0.1:%s/trace2.data", r.DataPort)
		//go r.ReadHttp(dataUrl2)
	}

	if r.HttpPort == "8001" {
		dataUrl2 := fmt.Sprintf("http://127.0.0.1:%s/trace2.data", r.DataPort)
		go r.ReadHttp(dataUrl2)
	}
	c.JSON(http.StatusOK, "ok")
	return
}

func (r *Receiver) QueryWrongHandler(c *gin.Context) {
	id := c.DefaultQuery("id", "")
	over := c.DefaultQuery("over", "0")
	r.wrongIdMap.Store(id, true)
	//if id == "c074d0a90cd607b" {
	//	r.logger.Info("got wrong example notify",
	//		zap.String("id", id),
	//	)
	//}
	tdi, exist := r.idToTrace.Load(id)
	if exist {
		// 存在,表示缓存还在
		// 等待过期即可
		if r.finishBool {
			r.logger.Info("should exist map ",
				zap.String("id", id),
			)
		}
		otd := tdi.(*common.TraceData)
		otd.Wrong = true
	} else {
		// 查找lru
		ltdi, lexist := r.lruCache.Get(id)
		if lexist {
			ltd := ltdi.(*common.TraceData)
			//b, _ := ltd.Marshal()
			//if ltd.GetStatusL() == common.TraceStatusDone {
			//	ltd.SetStatusL(common.TraceStatusSended)
			if over == "1" {
				//SendWrongRequestB(ltd, r.CompactorSetWrongUrl, b, "", nil)
				SendWrongRequest(ltd, r.CompactorSetWrongUrl, "", nil)
				r.logger.Info("query wrong in over",
					zap.String("id", id),
				)
			} else {
				r.lruCache.Remove(id)
				go func() {
					//SendWrongRequestB(ltd, r.CompactorSetWrongUrl, b, "", nil)
					SendWrongRequest(ltd, r.CompactorSetWrongUrl, "", nil)
				}()
			}
			//}
		}
	}
	c.JSON(http.StatusOK, "")
	return
}

//func (r *Receiver) ConsumeTraceData(spans common.Spans) {
//
//	idToSpans := make(map[string]*common.TraceData)
//	for _, span := range spans {
//		id := span.TraceId
//		span.TraceId = ""
//		if etd, ok := idToSpans[id]; !ok {
//			//tdi := r.tdPool.Get()
//			//td := tdi.(*common.TraceData)
//			td := &common.TraceData{
//				Sd:     []*common.SpanData{},
//				Source: r.HttpPort,
//			}
//			td.Id = id
//			td.Sd = append(td.Sd, span)
//			idToSpans[id] = td
//			//if !td.Wrong && span.Wrong {
//			//	td.Wrong = true
//			//}
//		} else {
//			//if !etd.Wrong && span.Wrong {
//			//	etd.Wrong = true
//			//}
//			etd.Sd = append(etd.Sd, span)
//		}
//	}
//
//	for id, etd := range idToSpans {
//		tdi, exist := r.idToTrace.LoadOrStore(id, etd)
//		if exist {
//			// 已存在
//			td := tdi.(*common.TraceData)
//			td.Add(etd.Sd)
//			//if !td.Wrong && etd.Wrong {
//			//	// noti
//			//	td.Wrong = true
//			//}
//			//etd.Clear()
//			//r.tdPool.Put(etd)
//		} else {
//			//if etd.Wrong {
//			//	// notify
//			//	mockTd := &common.TraceData{Id: id, Source: r.HttpPort, Status: common.TraceStatusInit}
//			//	go SendWrongRequest(mockTd, r.CompactorSetWrongUrl, "", nil)
//			//}
//			//etd.SetStatusL(common.TraceStatusReady)
//			// 淘汰一个
//			postDeletion := false
//			for !postDeletion {
//				select {
//				case r.deleteChan <- id:
//					postDeletion = true
//				default:
//					dropId, ok := <-r.deleteChan
//					if ok {
//						r.dropTrace(dropId, "0")
//					}
//				}
//			}
//		}
//	}
//}

func (r *Receiver) ConsumeByte(lines [][]byte) {

	idToSpans := make(map[string]*common.TraceData)
	for _, line := range lines {
		id := r.GetTraceIdFromByte(line)

		//id := span.TraceId
		//span.TraceId = ""
		if etd, ok := idToSpans[id]; !ok {
			//tdi := r.tdPool.Get()
			//td := tdi.(*common.TraceData)
			td := &common.TraceData{
				//Sd:     []*common.SpanData{},
				Source: r.HttpPort,
				Sb:     [][]byte{},
			}
			td.Id = id
			td.Sb = append(td.Sb, line)
			//td.Sd = append(td.Sd, span)
			idToSpans[id] = td
			//if !td.Wrong && span.Wrong {
			//	td.Wrong = true
			//}
			if !td.Wrong && r.IfSpanWrong(line) {
				td.Wrong = true
			}
		} else {
			//if !etd.Wrong && span.Wrong {
			//	etd.Wrong = true
			//}
			if !etd.Wrong && r.IfSpanWrong(line) {
				etd.Wrong = true
			}
			etd.Sb = append(etd.Sb, line)
			//etd.Sd = append(etd.Sd, span)
		}
	}

	for id, etd := range idToSpans {
		tdi, exist := r.idToTrace.LoadOrStore(id, etd)
		if exist {
			// 已存在
			td := tdi.(*common.TraceData)
			td.AddSpan(etd.Sb)
			if !td.Wrong && etd.Wrong {
				// noti
				td.Wrong = true
			}
			//etd.Clear()
			//r.tdPool.Put(etd)
		} else {
			//if etd.Wrong {
			//	// notify
			//	mockTd := &common.TraceData{Id: id, Source: r.HttpPort, Status: common.TraceStatusInit}
			//	go SendWrongRequest(mockTd, r.CompactorSetWrongUrl, "", nil)
			//}
			//etd.SetStatusL(common.TraceStatusReady)
			// 淘汰一个
			postDeletion := false
			for !postDeletion {
				select {
				case r.deleteChan <- id:
					postDeletion = true
				default:
					dropId, ok := <-r.deleteChan
					if ok {
						//r.idToTrace.Delete(dropId)
						r.dropTrace(dropId, "0")
					}
				}
			}
		}
	}
}

func (r *Receiver) dropTrace(id string, over string) {
	atomic.AddInt64(&r.traceNums, 1)

	d, ok := r.idToTrace.Load(id)
	if !ok {
		r.logger.Error("drop id not exist", zap.String("id", id))
		return
	}
	td := d.(*common.TraceData)
	//spLen := len(td.Sd)
	//if r.maxSpanNums < spLen {
	//	r.maxSpanNums = spLen
	//}
	//if r.minSpanNums > spLen || r.minSpanNums == 0 {
	//	r.minSpanNums = spLen
	//}
	wrong := td.Wrong
	if !wrong {
		_, ok := r.wrongIdMap.Load(td.Id)
		if ok {
			wrong = true
		}
	}
	r.idToTrace.Delete(id)
	if wrong {
		go SendWrongRequest(td, r.CompactorSetWrongUrl, over, &r.overWg)
		return
	}
	r.lruCache.Add(id, td)
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
				case id := <-r.deleteChan:
					r.dropTrace(id, "1")
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
	S1       = []byte("|")
)

func (r *Receiver) GetTraceIdFromByte(line []byte) string {
	firstIdx := bytes.Index(line, S1)
	return common.BytesToString(line[:firstIdx])
}

func (r *Receiver) IfSpanWrong(line []byte) bool {
	if bytes.Contains(line, Ferr1) {
		return true
	}
	if bytes.Contains(line, FCode) && !bytes.Contains(line, FCode200) {
		return true
	}
	return false
}

func (r *Receiver) ParseSpanData(line []byte) *common.SpanData {
	//spanDatai := r.spanPool.Get()
	//spanData := spanDatai.(*common.SpanData)
	spanData := &common.SpanData{}
	firstIdx := bytes.Index(line, S1)
	spanData.TraceId = common.BytesToString(line[:firstIdx])

	secondIdx := bytes.Index(line[firstIdx+1:], S1)
	spanData.StartTime = common.BytesToString(line[firstIdx+1 : firstIdx+1+secondIdx])

	spanData.Tags = line

	if bytes.Contains(line, Ferr1) {
		spanData.Wrong = true
		return spanData
	}
	if bytes.Contains(line, FCode) && !bytes.Contains(line, FCode200) {
		spanData.Wrong = true
	}
	return spanData
}
