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
	tdPool      *sync.Pool
	spanPool    *sync.Pool
	AutoDetect  bool
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

	r.tdPool = &sync.Pool{
		New: func() interface{} {
			return &common.TraceData{
				Sd:     []*common.SpanData{},
				Id:     "",
				Source: r.HttpPort,
			}
		},
	}

	r.spanPool = &sync.Pool{
		New: func() interface{} {
			return &common.SpanData{
				TraceId:   "",
				StartTime: "",
				Tags:      "",
				Wrong:     false,
			}
		},
	}

	// 10000条 = 2.9MB

	// 13*20*2.9 = 754
	// 300 * 2.9 = 870
	//r.lruCache, err = lru.NewWithEvict(5_0000, func(key interface{}, value interface{}) {
	//	td := value.(*common.TraceData)
	//	for _, v := range td.Sd {
	//		v.Clear()
	//		r.spanPool.Put(v)
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

	r.deleteChan = make(chan string, 5000)
	r.finishChan = make(chan interface{})
	doneFunc := func() {
		close(r.finishChan)
	}
	//r.consumer = NewChannelConsume(r, doneFunc)
	r.consumer = NewChannelGroupConsume(r, doneFunc)
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
		otd := tdi.(*common.TraceData)
		otd.Wrong = true
		if r.finishBool {
			r.logger.Info("should exist map ",
				zap.String("id", id),
			)
		}
	} else {
		// 查找lru
		ltdi, lexist := r.lruCache.Get(id)
		if lexist {
			ltd := ltdi.(*common.TraceData)
			if over == "1" {
				SendWrongRequest(ltd, r.CompactorSetWrongUrl, "", nil)
				r.logger.Info("query wrong in over",
					zap.String("id", id),
				)
			} else {
				r.lruCache.Remove(id)
				go func() {
					SendWrongRequest(ltd, r.CompactorSetWrongUrl, "", nil)
				}()
			}

		}
	}
	c.JSON(http.StatusOK, "")
	return
}

func (r *Receiver) ConsumeTraceData(spans common.Spans) {

	idToSpans := make(map[string]*common.TraceData)
	for _, span := range spans {
		id := span.TraceId
		span.TraceId = ""
		if etd, ok := idToSpans[id]; !ok {
			tdi := r.tdPool.Get()
			td := tdi.(*common.TraceData)
			td.Id = id
			td.Sd = append(td.Sd, span)
			idToSpans[id] = td
		} else {
			etd.Sd = append(etd.Sd, span)
		}
	}

	for id, etd := range idToSpans {
		//initialTraceData := &common.TraceData{
		//	Sd:     spans,
		//	Id:     id,
		//	Source: r.HttpPort,
		//}
		tdi, exist := r.idToTrace.LoadOrStore(id, etd)
		if exist {
			// 已存在
			td := tdi.(*common.TraceData)
			td.Add(etd.Sd)
			etd.Clear()
			r.tdPool.Put(etd)
		} else {
			postDeletion := false
			// 淘汰一个
			for !postDeletion {
				select {
				case r.deleteChan <- id:
					postDeletion = true
				default:
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
	atomic.AddInt64(&r.traceNums, 1)

	d, ok := r.idToTrace.Load(id)
	if !ok {
		r.logger.Error("drop id not exist", zap.String("id", id))
		return
	}
	td := d.(*common.TraceData)
	//if !keep {
	r.lruCache.Add(id, td)
	r.idToTrace.Delete(id)
	//td.Sd = common.Spans{}
	//}

	//spLen := len(td.Sd)
	//if r.maxSpanNums < spLen {
	//	r.maxSpanNums = spLen
	//}
	//if r.minSpanNums > spLen || r.minSpanNums == 0 {
	//	r.minSpanNums = spLen
	//}
	wrong := td.Wrong
	if !wrong {
		for _, span := range td.Sd {
			if span.Wrong {
				wrong = true
			}
		}
	}

	// 再次检测缓存map
	if !wrong {
		_, ok := r.wrongIdMap.Load(td.Id)
		if ok {
			wrong = true
		}
	}

	if wrong {
		r.lruCache.Remove(id)
		//r.logger.Info("send wrong id", zap.String("id", id))
		go SendWrongRequest(td, r.CompactorSetWrongUrl, over, &r.overWg)
		return
	}
	//r.lruCache.Add(id, td)
}

func (r *Receiver) finish() {
	<-r.finishChan
	btime := time.Now()
	r.logger.Info("start clear less")
	for {
		select {
		case id := <-r.deleteChan:
			r.dropTrace(id, "1")
		default:
			r.logger.Info("clear less succ",
				zap.Duration("cost", time.Since(btime)),
				zap.Int64("traceNum", r.traceNums),
				zap.Int("maxSpLen", r.maxSpanNums),
			)
			r.finishBool = true
			btime = time.Now()
			r.overWg.Wait()
			r.logger.Info("clear less over",
				zap.Duration("cost", time.Since(btime)),
				zap.Int64("traceNum", r.traceNums),
				zap.Int("maxSpLen", r.maxSpanNums),
				zap.Int("minSpanNums", r.minSpanNums),
			)
			r.notifyFIN()
			return
		}
	}
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

func (r *Receiver) ParseSpanData(line []byte) *common.SpanData {
	//spanDatai := r.spanPool.Get()
	//spanData := spanDatai.(*common.SpanData)

	spanData := &common.SpanData{}
	lineStr := common.BytesToString(line)
	words := strings.Split(lineStr, "|")
	if len(words) < 3 {
		return nil
	}
	spanData.TraceId = words[0]
	spanData.StartTime = words[1]

	//st, err := strconv.ParseInt(words[1], 10, 64)
	//if err != nil {
	//	fmt.Printf("timestamp to int64 fail %v \n", words[1])
	//	return nil
	//}

	//firstIdx := bytes.Index(line, S1)
	//spanData.TraceId = line[:firstIdx]
	//secondIdx := bytes.Index(line[firstIdx:],S1)
	spanData.Tags = lineStr
	if bytes.Contains(line, Ferr1) {
		spanData.Wrong = true
		return spanData
	}
	if bytes.Contains(line, FCode) && !bytes.Contains(line, FCode200) {
		spanData.Wrong = true
	}
	return spanData
}
