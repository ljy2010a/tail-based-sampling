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
	"runtime"
	"strings"
	"sync"
	"time"
)

type Receiver struct {
	HttpPort             string // 8000,8001
	DataPort             string // 8081
	CompactorPort        string // 8002
	CompactorSetWrongUrl string
	logger               *zap.Logger
	//idToTrace            sync.Map

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
		for {
			b2Mb := func(b uint64) uint64 {
				return b / 1024 / 1024
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			r.logger.Info("MEM STAT",
				zap.Uint64("Alloc", b2Mb(m.Alloc)),
				zap.Uint64("TotalAlloc", b2Mb(m.TotalAlloc)),
				zap.Uint64("HeapInuse", b2Mb(m.HeapInuse)),
				zap.Uint64("HeapAlloc", b2Mb(m.HeapAlloc)),
				zap.Uint64("Sys", b2Mb(m.Sys)),
				zap.Uint32("NumGC", m.NumGC),
			)
			time.Sleep(1 * time.Second)
		}
	}()
	//r.tdPool = &sync.Pool{
	//	New: func() interface{} {
	//		return &common.TraceData{
	//			Source: r.HttpPort,
	//			Status: common.TraceStatusReady,
	//			Sb:     [][]byte{},
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
	//r.lruCache, err = lru.NewWithEvict(5_0000, func(key interface{}, value interface{}) {
	//	td := value.(*common.TraceData)
	//	for i := range td.Sb {
	//		//bytebufferpool.Put(&bytebufferpool.ByteBuffer{B: td.Sb[i]})
	//		//td.Sd[i].Reset()
	//		//r.spanPool.Put(td.Sd[i])
	//	}
	//	//td.Clear()
	//	//r.tdPool.Put(td)
	//})
	r.lruCache, err = lru.New(15_0000)
	if err != nil {
		r.logger.Error("lru new fail",
			zap.Error(err),
		)
	}

	r.deleteChan = make(chan string, 8000)
	r.finishChan = make(chan interface{})
	doneFunc := func() {
		//r.lruCache.Resize(15_0000)
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
	//tdi, exist := r.lruCache.Get(id)
	//if exist {
	//	// 存在,表示缓存还在
	//	// 等待过期即可
	//	if r.finishBool {
	//		r.logger.Info("should exist map ",
	//			zap.String("id", id),
	//		)
	//	}
	//	otd := tdi.(*common.TraceData)
	//	otd.Wrong = true
	//} else {
	// 查找lru
	ltdi, lexist := r.lruCache.Get(id)
	if lexist {
		ltd := ltdi.(*common.TraceData)
		ltd.Wrong = true
		if ltd.Status == common.TraceStatusDone {
			ltd.Status = common.TraceStatusSended
			//b, _ := ltd.Marshal()
			if over == "1" {
				//SendWrongRequestB(ltd, r.CompactorSetWrongUrl, b, "", nil)
				SendWrongRequest(ltd, r.CompactorSetWrongUrl, "", nil)
				//r.logger.Info("query wrong in over",
				//	zap.String("id", id),
				//)
			} else {
				//r.lruCache.Remove(id)
				go func() {
					//SendWrongRequestB(ltd, r.CompactorSetWrongUrl, b, "", nil)
					SendWrongRequest(ltd, r.CompactorSetWrongUrl, "", nil)
				}()
			}
		}
	}

	//spanBytei, ok := r.lruCache.Get(id)
	//if ok {
	//	spans := bytes.Split(spanBytei.([]byte), []byte("\n"))
	//	td := &common.TraceData{
	//		Id:     id,
	//		Source: r.HttpPort,
	//		Sb:     spans,
	//	}
	//	if over == "1" {
	//		SendWrongRequest(td, r.CompactorSetWrongUrl, "", nil)
	//		r.logger.Info("query wrong in over",
	//			zap.String("id", id),
	//		)
	//	} else {
	//		r.lruCache.Remove(id)
	//		go func() {
	//			SendWrongRequest(td, r.CompactorSetWrongUrl, "", nil)
	//		}()
	//	}
	//}
	//}
	c.JSON(http.StatusOK, "")
	return
}

func (r *Receiver) ConsumeByte(lines [][]byte) {
	idToSpans := make(map[string]*common.TraceData)
	for i := range lines {
		line := lines[i]
		id := GetTraceIdFromByte(line)
		if etd, ok := idToSpans[id]; !ok {
			//tdi := r.tdPool.Get()
			//td := tdi.(*common.TraceData)
			//td.Wrong = r.IfSpanWrong(line)
			td := &common.TraceData{
				Source: r.HttpPort,
				Sb:     [][]byte{},
				Wrong:  IfSpanWrong(line),
				//Status: common.TraceStatusReady,
			}
			td.Id = id
			//td.AddOne(line)
			td.Sb = append(td.Sb, line)
			idToSpans[id] = td
		} else {
			if !etd.Wrong && IfSpanWrong(line) {
				etd.Wrong = true
			}
			//etd.AddOne(line)
			etd.Sb = append(etd.Sb, line)
		}
	}

	for id, etd := range idToSpans {
		exist, _ := r.lruCache.ContainsOrAdd(id, etd)
		if exist {
			// 已存在
			tdi, texist := r.lruCache.Get(id)
			if !texist {
				r.logger.Info("t not exist ", zap.String("id", id))
			} else {
				td := tdi.(*common.TraceData)
				td.AddSpan(etd.Sb)
				if !td.Wrong && etd.Wrong {
					td.Wrong = true
				}
			}
			//etd.Clear()
			//r.tdPool.Put(etd)
		} else {
			postDeletion := false
			for !postDeletion {
				select {
				case r.deleteChan <- id:
					postDeletion = true
				default:
					//<-r.deleteChan
					dropId, ok := <-r.deleteChan
					if ok {
						r.dropTrace(dropId, "0")
					}
				}
			}
		}
	}
}

func (r *Receiver) dropTrace(id string, over string) {
	//atomic.AddInt64(&r.traceNums, 1)

	d, ok := r.lruCache.Get(id)
	if !ok {
		r.logger.Info("drop id not exist", zap.String("id", id))
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
	if wrong && td.Status != common.TraceStatusSended {
		td.Status = common.TraceStatusSended
		go SendWrongRequest(td, r.CompactorSetWrongUrl, over, &r.overWg)
		return
	} else {
		td.Status = common.TraceStatusDone
	}
	//r.lruCache.Add(id, td)
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
					r.dropTrace(dropId, "1")
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

func GetTraceIdWrongFromString(line []byte) (string, bool) {
	l := common.BytesToString(line)
	id := l[:strings.Index(l, "|")]
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

func GetTraceIdWrongFromByte(line []byte) (string, bool) {
	l := common.BytesToString(line)
	id := l[:strings.IndexByte(l, '|')]
	if bytes.Contains(line, Ferr1) {
		return id, true
	}
	if bytes.Contains(line, FCode) && !bytes.Contains(line, FCode200) {
		return id, true
	}
	return id, false
}

func GetTraceIdFromByte(line []byte) string {
	firstIdx := bytes.IndexByte(line, '|')
	return common.BytesToString(line[:firstIdx])
}

func IfSpanWrong(line []byte) bool {
	//l := common.BytesToString(line)
	//if strings.Contains(l, "error=1") {
	//	return true
	//}
	//pos := strings.Index(l, "http.status_code=")
	//if pos == -1 {
	//	return false
	//}
	//if l[pos:pos+3] != "200" {
	//	return true
	//}
	//return false

	//if strings.Contains(l, "http.status_code=") && !strings.Contains(l, "http.status_code=200") {
	//	return true
	//}

	if bytes.Contains(line, Ferr1) {
		return true
	}
	if bytes.Contains(line, FCode) && !bytes.Contains(line, FCode200) {
		return true
	}
	return false
}
