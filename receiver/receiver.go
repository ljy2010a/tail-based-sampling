package receiver

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/smallnest/goreq"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Receiver struct {
	HttpPort      string // 8000,8001
	DataPort      string // 8081
	CompactorPort string // 8002
	logger        *zap.Logger
	idToTrace     sync.Map

	deleteChan  chan string
	finishChan  chan interface{}
	closeTimes  int64
	consumerLen int64
	sync.Mutex

	wrongIdMap sync.Map
	lruCache   *lru.Cache
	lineChan   chan []byte
}

var (
	compactorReq = goreq.New()
)

func (r *Receiver) Run() {
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

				//dataUrl2 := fmt.Sprintf("http://127.0.0.1:%s/trace2.data", r.DataPort)
				//go r.ReadHttp(dataUrl2)
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

	//cache, err := ristretto.NewCache(&ristretto.Config{
	//	NumCounters: 1e7,     // number of keys to track frequency of (10M).
	//	MaxCost:     1 << 30, // maximum cost of cache (1GB).
	//	BufferItems: 64,      // number of keys per Get buffer.
	//})
	r.lruCache, err = lru.New(100000)
	if err != nil {
		r.logger.Error("lru new fail",
			zap.Error(err),
		)
	}

	r.lineChan = make(chan []byte, 50000)
	r.deleteChan = make(chan string, 30000)
	r.finishChan = make(chan interface{})
	r.consumerLen = 2

	go r.finish()
	for i := int64(0); i < r.consumerLen; i++ {
		go r.ConsumeTraceByteAsync()
	}

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
	r.logger.Info("ready", zap.String("port", r.HttpPort))
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
	// 暂时用一个
	if r.HttpPort == "8000" {
		dataUrl := fmt.Sprintf("http://127.0.0.1:%s/trace1.data", r.DataPort)
		//r.logger.Info("gen dataUrl", zap.String("dataUrl", dataUrl))
		go r.ReadHttp(dataUrl)

		//dataUrl2 := fmt.Sprintf("http://127.0.0.1:%s/trace2.data", r.DataPort)
		////r.logger.Info("gen dataUrl", zap.String("dataUrl", dataUrl2))
		//go r.ReadHttp(dataUrl2)
	}

	if r.HttpPort == "8001" {
		dataUrl2 := fmt.Sprintf("http://127.0.0.1:%s/trace2.data", r.DataPort)
		//r.logger.Info("gen dataUrl", zap.String("dataUrl", dataUrl2))
		go r.ReadHttp(dataUrl2)
	}
	c.JSON(http.StatusOK, "ok")
	return
}

func (r *Receiver) QueryWrongHandler(c *gin.Context) {
	id := c.DefaultQuery("id", "")
	r.wrongIdMap.Store(id, true)
	if id == "c074d0a90cd607b" {
		r.logger.Info("got wrong example notify",
			zap.String("id", id),
		)
	}
	tdi, exist := r.idToTrace.Load(id)
	if exist {
		// 存在,表示缓存还在
		// 等待过期即可
		otd := tdi.(*common.TraceData)
		otd.Wrong = true

		// 已过期
		//if len(otd.Sd) == 0 {
		//r.logger.Info("expire id",
		//	zap.String("id", id),
		//	zap.String("port", r.HttpPort),
		//)
		//}

	} else {
		// 未出现
		//r.logger.Info("no cache id",
		//	zap.String("id", id),
		//	zap.String("port", r.HttpPort),
		//)
		// 查找lru
		ltdi, lexist := r.lruCache.Get(id)
		if lexist {
			//r.logger.Info(" id found in lru",
			//	zap.String("id", id),
			//	zap.String("port", r.HttpPort),
			//)
			//r.lruCache.Remove(id)
			ltd := ltdi.(*common.TraceData)
			go func() {
				swUrl := fmt.Sprintf("http://127.0.0.1:%s/sw", r.CompactorPort)
				compactorReq.Post(swUrl).SendStruct(ltd).End()
			}()
		} else {
			//r.logger.Info(" id not in lru",
			//	zap.String("id", id),
			//	zap.String("port", r.HttpPort),
			//)
		}
	}
	c.JSON(http.StatusOK, "")
	return
}

func (r *Receiver) ConsumeTraceData(spans []*common.SpanData) {

	idToSpans := make(map[string][]*common.SpanData)
	for _, span := range spans {
		id := span.TraceId
		idToSpans[id] = append(idToSpans[id], span)
		span.TraceId = ""
	}

	for id, spans := range idToSpans {
		initialTraceData := &common.TraceData{
			Sd:     spans,
			Id:     id,
			Source: r.HttpPort,
		}
		d, exist := r.idToTrace.LoadOrStore(id, initialTraceData)
		if exist {
			// 已存在
			//r.logger.Debug("exist id", zap.String("id", id))
			td := d.(*common.TraceData)
			td.Add(spans)
		} else {
			postDeletion := false
			currTime := time.Now()
			// 淘汰一个
			for !postDeletion {
				select {
				case r.deleteChan <- id:
					postDeletion = true
				default:
					dropId, ok := <-r.deleteChan
					if ok {
						r.dropTrace(dropId, currTime, false)
					}
				}
			}
		}
	}
}

func (r *Receiver) ConsumeTraceByteAsync() {
	btime := time.Now()
	size := 0
	wrong := 0
	groupNum := 5000
	spanDatas := make([]*common.SpanData, groupNum)
	i := 0
	go func() {
		r.logger.Info("read stat",
			zap.Int("wrong", wrong),
		)
		time.Sleep(10 * time.Second)
	}()
	once := sync.Once{}
	for line := range r.lineChan {
		once.Do(func() {
			btime = time.Now()
		})
		size++
		spanData := common.ParseSpanData(line)
		if spanData == nil {
			continue
		}
		if spanData.Wrong {
			wrong++
		}
		if i < groupNum {
			spanDatas[i] = spanData
		}
		if i == groupNum-1 {
			r.ConsumeTraceData(spanDatas)
			i = 0
			continue
		}
		i++
	}
	if i != 0 {
		r.ConsumeTraceData(spanDatas[:i])
	}
	r.logger.Info("deal file done ",
		zap.Int("wrong", wrong),
		zap.Int("dealSize", size),
		zap.Duration("cost", time.Since(btime)),
	)
	times := atomic.AddInt64(&r.closeTimes, 1)
	if times == r.consumerLen {
		close(r.finishChan)
	}
}

func (r *Receiver) dropTrace(id string, duration time.Time, keep bool) {
	d, ok := r.idToTrace.Load(id)
	if !ok {
		r.logger.Error("drop id not exist", zap.String("id", id))
		return
	}
	td := d.(*common.TraceData)
	//if !keep {
	r.idToTrace.Delete(id)
	r.lruCache.Add(id, td)
	//td.Sd = common.Spans{}
	//}

	wrong := td.Wrong
	for _, span := range td.Sd {
		if span.Wrong {
			wrong = true
		}
	}

	// 再次检测缓存map
	if !wrong {
		_, ok := r.wrongIdMap.Load(td.Id)
		if ok {
			// 遗漏
			wrong = true
		}
	}

	if wrong {
		//r.Lock()
		// send2compactor no keep
		//r.logger.Info("send wrong id", zap.String("id", id))
		swUrl := fmt.Sprintf("http://127.0.0.1:%s/sw", r.CompactorPort)
		go SendWrongRequest(td, swUrl)
		//compactorReq.Post(swUrl).SendStruct(td).End()
		//r.Unlock()
		//b, _ := json.Marshal(td)
		//go func(_b []byte) {
		//	compactorReq.Post(swUrl).SendRawBytes(_b).End()
		//}(b)
		return
	}
}

func (r *Receiver) finish() {
	<-r.finishChan
	btime := time.Now()
	r.logger.Info("start clear less")
	for {
		select {
		case id := <-r.deleteChan:
			r.dropTrace(id, time.Now(), true)
		default:
			r.logger.Info("clear less succ",
				zap.Duration("cost", time.Since(btime)),
			)
			r.notifyFIN()
			return
		}
	}
}

func (r *Receiver) notifyFIN() {
	// send fin
	_, body, err := compactorReq.Get(fmt.Sprintf("http://127.0.0.1:%s/fn?port=%s", r.CompactorPort, r.HttpPort)).End()
	r.logger.Info("send notify fin",
		zap.String("body", body),
		zap.Errors("err", err),
	)
	r.logger.Info("shutdown", zap.String("port", r.HttpPort))
}

func SendWrongRequest(td *common.TraceData, reqUrl string) {

	b, _ := json.Marshal(td)
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(reqUrl)
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")
	req.SetBody(b)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := fasthttp.Do(req, resp); err != nil {
		fmt.Printf("set wrong fail id[%v] err[%v] \n", td.Id, err)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		fmt.Printf("set wrong fail[%v] code[%v]\n", td.Id, resp.StatusCode())
		return
	}

}
